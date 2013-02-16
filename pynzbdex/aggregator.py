from datetime import datetime
import time
import logging
import sys
import pytz
import traceback
import re
import itertools

import riak
import dateutil.parser
import iso8601
from redis import StrictRedis
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import DeferredReflection
from sqlalchemy.sql import expression
try:
    import cPickle as pickle
except ImportError:
    import pickle

from pynzbdex.pynntpcli import NNTPProxyClient
from pynzbdex import storage, settings
storage.riak.BACKEND = 'PBC'

import warnings
warnings.simplefilter('error')


log = logging.getLogger(__name__)


# don't move these to settings for now.. yes they are
# for tuning, but there are so many tunings that won't work
# that this may not be configurable
redis_indexbuckets = 1000
nntp_chunksize = 1000
riak_chunksize = 1000
sql_chunksize = 10000
# when querying ranges, the max we'll span
# (anything larger needs to be divided and aggregated)
range_stepsize = 10000


class FieldParsers(object):
    ## a collection of classmethods to handle complex field normalization.
    ## this might get offloaded into the `storage` module..
    @classmethod
    def commalist(kls, value):
        if value:
            return value.split(u',')
        return []

    @classmethod
    def parsedate(kls, value):
        if value:
            dt = None
            ## try all these formats
            # whatever this is: 09 Dec 2012 20:36:13 GMT
            # or this:          Wed, 14 Mar 2012 14:25:28 GMT
            # or this:          Wed, 14 Mar 2012 11:26:08 -0500
            parsers = [
                lambda x: datetime.strptime(x, '%d %b %Y %H:%M:%S %Z'),
                lambda x: datetime.strptime(x, '%a, %d %b %Y %H:%M:%S %Z'),
                iso8601.parse_date,
                dateutil.parser.parse
            ]
            for p in parsers:
                try:
                    dt = p(value)
                except Exception, e:
                    continue
                else:
                    return (dt.astimezone(pytz.utc).replace(tzinfo=None)
                                if dt.tzinfo else dt)
            raise Exception('Could not parse date')

        return value


## fake riak objects for use in things like deleting without
## needing to first retrieve the object (say if you have a list
## of keys coming back from an MR operation)
class FakeRiakObject(object):
    def __init__(self, bucket, key, vclock):
        self._bucket = bucket
        self._key = key
        self._vclock = vclock

    def get_key(self):
        return self._key

    def get_bucket(self):
        return self._bucket

    def vclock(self):
        return self._vclock

class FakeRiakBucket(object):
    def __init__(self, name):
        self._name = name


class Aggregator(object):
    __nntp = None
    __redis = None
    __sql = None

    def __init__(self, redis_pool=None, *args, **kwargs):
        '''
        all connectors should nicely handle all our reconnection bs for us.
        or if it doesn't it will be patched to do so =)
        I guess if we had args for the clients they'd go in here..
        '''
        pass

    @property
    def _nntp(self):
        if not self.__nntp:
            nntp_cfg = settings.NNTP_PROXY['default']
            self.__nntp = NNTPProxyClient(
                                      host=nntp_cfg['HOST'],
                                      port=nntp_cfg['PORT'],
                                      pickle_protocol=settings.PICKLE_PROTOCOL)
        return self.__nntp

    @property
    def _redis(self):
        if not self.__redis:
            redis_cfg = settings.REDIS['default']
            self.__redis = StrictRedis(host=redis_cfg['HOST'],
                                       port=redis_cfg['PORT'],
                                       db=redis_cfg['DB'],
                                       decode_responses=True)
        return self.__redis

    @property
    def _sql(self):
        if not self.__sql:
            sql_cfg = settings.DATABASE['default']
            dsn = '%(DIALECT)s+%(DRIVER)s://%(USER)s:%(PASS)s@%(HOST)s:%(PORT)s/%(NAME)s?charset=utf8' % sql_cfg
            sql_engine = create_engine(dsn)
            storage.sql.Base.metadata.create_all(sql_engine)
            DeferredReflection.prepare(sql_engine)
            Session = sessionmaker(bind=sql_engine) #, autoflush=False) #, autocommit=True)
            self.__sql = Session()
        return self.__sql

    def cache_article_to_redis(self, article_d):
        '''
        Given an article document, this constructs a new object
        comprised of a small subset of fields.  A sister process
        will be monitoring redis for new items in order to populate
        the RDBMS with relational data.
        '''
        key = article_d.key
        ##TODO: bytes field is for the entire raw post, including headers.
        ##should calculate raw header size and then subtract from field value
        ##to get a closer approximation of the file size.

        mdata = {
                'from_': article_d.from_,
                'bytes_': article_d.bytes_,
                'subject': article_d.subject,
                'date': time.mktime(article_d.date.timetuple()),
                'newsgroups': article_d.newsgroups,
            }

        try:
            msg = pickle.dumps(mdata, protocol=settings.PICKLE_PROTOCOL)
        except pickle.PicklingError:
            log.error(mdata)
            raise

        self._redis.set(key, msg)
        self._redis.sadd('newart:%s' % (hash(key) % redis_indexbuckets), key)
        
    def expire_article_to_redis(self, mesg_spec):
        '''
        The redis store (and subsequently the RDBMS) keys articles (correctly)
        on the message spec.  This generates a simple "message" to Redis 
        representing the intent that the item should be marked as deleted.
        This is NOT expiring an item in redis but is instead sending a message
        via the cache layer that it should be expired from the RDBMS.

        (the whole redis layer is simply to prevent slowing down the
        nntp indexer)
        '''
        self._redis.set(mesg_spec, 'EXPIRE')
        self._redis.sadd('newart:%s' % (hash(mesg_spec) % redis_indexbuckets),
                         mesg_spec)

    def delete_article_range(self, group_name, start, end, inclusive=False):
        log.info('Deleting articles (%s) [%s:(%s, %s)]' \
                    % ( ('inclusive' if inclusive else 'exclusive'),
                        group_name, start, end))

        ## in addtion to chunks returned we have to break up the search
        ## space as well.
        range_step = (end - start)
        if range_step > range_stepsize:
            range_step = range_stepsize

        keys_deleted = 0
        if inclusive:
            rpat = '[%s TO %s]'
        else:
            rpat = '{%s TO %s}'
        start_cursor = start
        end_cursor = start + range_step
        while end_cursor <= end:
            range_term = rpat % (start_cursor, end_cursor)
            text_query = 'xref_%s_int:%s' % (group_name.replace('.', '_'),
                                             range_term)
            docs_found = None
            start_index = 0
            log.debug('DELETE QUERY: %s' % text_query)
            while docs_found != 0:
                q = None
                for i in range(0,5):
                    try:
                        q = storage.riak.Article.solrSearch(text_query,
                                                       start=start_index,
                                                       rows=riak_chunksize)
                    except riak.RiakError:
                        log.info('Error reaching Riak for Solr Query'
                                 ' [%s](%s, %s)' % (text_query, start_index,
                                                    riak_chunksize))
                        log.info(traceback.format_exc())
                        time.sleep(1)
                    else:
                        break

                if q is None:
                    raise Exception('Gave up trying to query Riak Solr')

                docs_found = q.length()

                if docs_found:
                    log.debug('Articles remaining: %s (%s, %s)' % (
                                                docs_found,
                                                start_index,
                                                start_index + riak_chunksize))
                ## delete docs
                chunk_keys_deleted = 0
                for ad in q.all():
                    key = ad.key
                    log.debug('Deleting article: %s' % key)

                    ad.delete()
                    self.expire_article_to_redis(key)

                    chunk_keys_deleted += 1

                keys_deleted += chunk_keys_deleted
                ## stuff could easily get added to the range we are deleting from
                ## while we're working....  nothing to do about this but take a
                ## multi-pass approach.
                if (    docs_found
                        and chunk_keys_deleted < riak_chunksize
                        and chunk_keys_deleted < docs_found):
                    log.debug('Reset')
                    start_index = 0
                elif chunk_keys_deleted > riak_chunksize:
                    raise RuntimeError('Holy S')
                else:
                    start_index += riak_chunksize

            if keys_deleted:
                log.info('Deleted %s articles (%s) [%s:(%s, %s)]' \
                            % ( keys_deleted,
                                ('inclusive' if inclusive else 'exclusive'),
                                group_name, start_cursor, end_cursor))

            start_cursor += range_step
            ## b/c I am lazy about remainders, if we overshoot,
            ## allow for one final iteration to get the remainder.
            if end_cursor == end:
                break
            else:
                end_cursor += range_step
                if end_cursor > end:
                    end_cursor = end

        return keys_deleted

    def scrape_groups(self, prefix='alt.binaries.*', refresh=True,
                      invalidate=False):
        ## scans the groups we know and prunes ones that do not exist
        ## grouplist is not prohibitvely large as article list,
        ## so we can just do it like this.... article list will take
        ## some different tactics... but is actually easier due to first/last
        ## article ID..
        if not prefix.endswith('.*'):
            log.warning('Got dangling prefix, appending ".*"')
            prefix = '%s.*' % prefix
        else:
            prefix = prefix

        stats = {
                'processed': 0,
                'updated': 0,
                'deleted': 0
            }

        ## so this is just a list of groupnames from the NNTP server
        groups = self._nntp.get_groups(prefix)

        ## updates our store of groups based on a prefix.
        log.info('Updating group store for prefix %s' % prefix)
        nntp_groups = []
        for group in groups:
            nntp_groups.append(group['group'])

            stats['processed'] += 1

            if refresh:
                log.debug('Updating group: %s' % group['group'])
                group_d = storage.riak.Group.getOrNew(group['group'],
                                                **{'name': group['group'],
                                                   'first': group['first'],
                                                   'last': group['last'],
                                                   'flag': group['flag']})
                ## group list doesn't return count.. only the get 
                ## single group groups style does...
                group_d.save()

                stats['updated'] += 1

        '''
        http://www.net.berkeley.edu/dcns/usenet/alt-creation-guide.html

        Newsgroup names which have components that are composed of the
        characters other than the letters 'a' through 'z', plus the characters
        '-' and '+' are considered non-standard and not encouraged.

        ...

        Newsgroup Longevity:
        There are some people who insist that once an alt newsgroup is created,
        it can never be destroyed, no matter what. These people make sure that 
        whenever someone tries to remove a group, it gets re-created. Even if
        these people were not on the net, occasional mistakes (in such
        situations as people setting up new sites) can cause almost-dead
        newsgroups to get revived everywhere. Thus, alt groups are effectively 
        immortal, at least for the foreseeable future; they can't be removed
        or even re-named. Alt groups never die, they just fade away. However,
        some alt groups fade away faster than others.

        ---
        In other words, without an exceedingly cheap algorithm for invalidating
        groups, it's not worth doing.
        '''

        if invalidate:
            log.info('Invalidating groups, got %s groups from NNTP server' \
                        % len(nntp_groups))
            text_query = 'name:%s' % prefix
            start_index = 0
            doc_groups = []
            log.debug('Riak Query: %s' % text_query)
            chunksize = 5000
            while 1:
                log.debug('Riak Chunk: %s, %s' % (start_index, chunksize))
                q = storage.riak.Group.solrSearch(text_query,
                                               start=start_index,
                                               rows=chunksize)

                chunk_count = 0
                for grp in q.all():
                    doc_groups.append(grp.key)
                    chunk_count += 1

                if not chunk_count:
                    break

                start_index += chunk_count

            dead_groups = set(doc_groups) - set(nntp_groups)

            frb = FakeRiakBucket(storage.riak.Group.bucket_name)
            for k in dead_groups:
                log.info('Deleting group: %s' % k)
                fro = FakeRiakObject(frb, k, None)
                storage.riak.client.get_transport().delete(fro)
                stats['deleted'] += 1

        return stats

    def scrape_articles(self, group_name, get_long_headers=False,
                        invalidate=False, cached=True, offset=0,
                        resume=False, max_processed=None):

        group_name = unicode(group_name)
        ## no idea what happens here if we pass an invalid groupname
        group = self._nntp.group(group_name)
        group_d = storage.riak.Group.getOrNew(group['group'],
                                        **{'count': group['count'],
                                            'first': group['first'],
                                            'last': group['last']})
        ## group get/switch doesn't return flag..
        ## only the getgroups list style does
        group_d.save()

        updated_articles = 0
        processed_articles = 0
        deleted_articles = 0

        first = group_d.last_stored if resume else group_d.first
        if not first: first = 0
        first += offset

        last = group_d.last
        ## if we're up to date, or there was article loss on the 
        ## server since the last time we tried to get it.. 
        if first > group_d.last:
            first = group_d.last
        elif first < group_d.first:
            first = group_d.first

        ## only waste time on doing this if we're doing a full pass AND we're
        ## looking to invalidate articles.
        if invalidate and not resume:
            ## so first, immediately kill all articles in this group,
            ## whose article number is less than what the refereshed group says
            ## is the first article.. this is the easy part.
            deleted_articles += self.delete_article_range(
                                            group_name, 0,
                                            group_d.first, inclusive=False)


        ## scrapes articles for a single group.
        log.info('Scraping articles for: [%s:(%s, %s)]' % (group_name, first, last))
        ### we need to do this in chunks b/c I don't yet know how big of a
        ### request I can make to NNTP
        ### get 1000 article short_headers at a time
        cursor_f = first
        cursor_l = cursor_f + nntp_chunksize
        if cursor_l > last: cursor_l = last
        previous_article_num = None

        while ((cursor_l <= last)
                    and
                (   max_processed == None
                        or 
                    (max_processed != None and processed_articles < max_processed))):

            articles = self._nntp.get_group((cursor_f, cursor_l), group_name)
            log.info('Got %s articles for %s between (%s, %s)' \
                        % (len(articles), group_name, cursor_f, cursor_l))

            ## break out on remaining indivisible chunk:
            log.debug('CF %d CL %d L %d' % (cursor_f, cursor_l, last))
            if cursor_f == cursor_l == last:
               previous_article_num = last
               break 
            else:
                ## increment..
                cursor_f = cursor_l
                cursor_l += nntp_chunksize
                if cursor_l > last: cursor_l = last

            ## next, if not in resume mode,  look for
            ## noncontiguous missing articles, for instance ones
            ## that have been specifically taken down, while we're in here
            ## updating others.
            for article_num, short_headers in articles:
                if (    max_processed != None
                        and processed_articles >= max_processed):
                    break
                log.debug('Processing article: %s:%s' % (group_name,
                                                         article_num))
                for k, v in short_headers.items():
                    if isinstance(v, str):
                        short_headers[k] = unicode(v, 'utf8')

                if invalidate:
                    # if in invalidation mode, then look for gaps in article
                    # IDs.  per RFC3977: "If the information is available,
                    # it is returned ... sorted in numerical order of article
                    # number"
                    if (previous_article_num is not None
                            and article_num - previous_article_num > 1):
                        deleted_articles += self.delete_article_range(
                                                group_name,
                                                previous_article_num,
                                                article_num, inclusive=False)

                ## CREATION AND SHORT HEADERS
                key = short_headers['message-id']
                norm_headers = {
                        'article_num': article_num,
                        'lines': short_headers['lines'],
            
                        'bytes_': short_headers['bytes'],
                        'xref': short_headers['xref'],
                        'from_': short_headers['from'],
                        'subject': short_headers['subject'],
                        'date': FieldParsers.parsedate(short_headers['date']),
                        'references': short_headers['references'],
                        'newsgroups': [group_name, ]
                    }

                article_d = storage.riak.Article.getOrNew(key)
                #has_bad_xref_tag = False

                xref_bits = [a.strip().split(':', 1)
                                for a in short_headers['xref'].split(' ')][1:]
                for g, i in xref_bits:
                    #if hasattr(article_d, 'xref_%s_int' % g):
                    #    has_bad_xref_tag = True

                    ## use int suffix to tie into default schema..
                    k = 'xref_%s_int' % g.replace('.', '_')
                    norm_headers[k] = int(i)
                    norm_headers['newsgroups'].append(g)

                '''
                if has_bad_xref_tag:
                    log.error('>>>>>>> HAS BAD XREF TAG <<<<<<<')
                    article_d.delete()
                    article_d = storage.riak.Article(key)
                else:
                    log.debug('<<<<<<< XREF TAGS CLEAN >>>>>>>>')
                '''
                previous_article_num = article_num
                processed_articles += 1

                ## cache check.. don't reindex if date and bytes match
                if cached:
                    if (    article_d._obj
                                and
                            article_d.date >= norm_headers['date'] 
                                and
                            article_d.bytes_ == int(norm_headers['bytes_'])
                                and not
                            ### if we got asked for long headers and they're
                            ### not in the cache, still a miss
                            ### else continue..
                            (get_long_headers and not article_d.nn_long_headers)
                        ):
                        log.debug('Article %s already cached' % key)
                        continue

                log.debug('Article %s updated' % key)
                #log.debug(short_headers)
                updated_articles += 1
                ## LONG HEADERS

                if get_long_headers: ## this happens one at a time..
                    ## i wonder if getting by article # is any faster than
                    ## msgid?, or vice versa?
                    long_headers = self._nntp.get_header(key)
                    for k, v in long_headers.items():
                        if isinstance(v, str):
                            long_headers[k] = unicode(v, 'utf8')

                    ## oh fuck me long_headers are not 100% consistent
                    ## across posts
                    #log.debug(long_headers)
                    ## these get handled further down or excluded.
                    ## for instance we always honor xref and ignore
                    ## current group + message id
                    special_keys = ['message-id', 'newsgroups', 'date',
                                    'from', 'bytes', ]
                    for key in [k for k in long_headers.keys()
                                                    if k not in special_keys]:
                        key_d = key.lstrip(':').replace('-', '_')
                        ## yes I know Riak can handle arbiatrary keys but right
                        ## now I want to KNOW if there's a field I don't know
                        ## about
                        if not hasattr(storage.riak.Article, key_d):
                            raise ValueError('New header field on Article doc:'
                                             ' %s' % key)
                        norm_headers[key_d] = long_headers[key]

                    ## redundant if long-headers unless the newsgroup field in
                    ## the headers is incomplete... riakkit uses set-type for
                    ## lists so we don't need to track redundance. This is an
                    ## intentionally destructive update.
                    if 'newsgroups' in long_headers:
                        norm_headers['newsgroups'].extend(
                                FieldParsers.commalist(long_headers['newsgroups']))
                    if 'date' in long_headers:
                        norm_headers['date'] = \
                                FieldParsers.parsedate(long_headers['date'])
                    if 'from' in long_headers:
                        norm_headers['from_'] = long_headers['from']
                    if 'bytes' in long_headers:
                        norm_headers['bytes_'] = long_headers['bytes']
                    ## flag that we've collected all the headers
                    ## there are.
                    norm_headers['nn_long_headers'] = True

                ## save out and increment
                article_d.mergeData(norm_headers)
                saved = None
                for i in range(0, 10):
                    try:
                        saved = article_d.save()
                    except Exception, e:
                        time.sleep(0.5)
                        continue
                    else:
                        break
                if not saved:
                    raise Exception('Could not store to Riak')

                self.cache_article_to_redis(article_d)

                log.debug('Article %s updated' % key)


            ## at the end of each chunk, update the group record with our
            ## progress. yes this does mean that we'll lose our place up to
            ## `nntp_chunksize` if it crashes...
            #group_d.reload()
            # yes you will concievably end up with gapping, but the
            # non-resuming scanners will fill it in..

            if resume and previous_article_num:
                group_d.last_stored = previous_article_num
                log.info('LAST STORED: %s' % group_d.last_stored)
                group_d.save()

        log.warning('last(%s) vs anum(%s)' % (group_d.last, previous_article_num))
        return {    'updated': updated_articles,
                    'deleted': deleted_articles,
                    'processed': processed_articles,
                    'last_message_id': previous_article_num, 
                    'complete': (previous_article_num >= group_d.last)}

    def process_redis_cache(self, group_name, step=1, *args, **kwargs):
        ## this reads the redis cache and stores the resulting
        ## message buckets to the RDBMS -- it's decoupled
        ## through redis with the assumption that this will
        ## be run from a separate thread or process than
        ## the article scraper.

        log.info('Processing Redis Keyspace (%s)' % step)
        ## cache of group objects
        GROUPS = {}
        stats = {
            'processed': 0,
            'updated': 0,
            'expired': 0,
        }

        # step offset to reduce sql collisions
        for b in xrange((step - 1), redis_indexbuckets, step):
            set_key = 'newart:%s' % b
            log.debug('Processing Redis Index Bucket: %s' % set_key)
            ## ONE BUCKET
            for mesg_spec in self._redis.smembers(set_key):
                stats['processed'] += 1

                art_msg = self._redis.get(mesg_spec)
                ## our sets persist longer than our keys
                if not art_msg:
                    ## clean up my mistakes
                    self._redis.srem(set_key, mesg_spec)
                    continue

                try:
                    ## immediately remove to cut down on contention
                    self._redis.srem(set_key, mesg_spec)
                    self._redis.delete(mesg_spec)
                    log.debug('REDIS POP: %s, %s' % (set_key, mesg_spec))

                    if art_msg.startswith('EXPIRE'):
                        log.debug('EXPIRED FROM RDBMS: %s' % mesg_spec)
                        ## this should check if the File (if any) associated
                        ## with this thing still has any other assocated
                        ## records and delete it if not
                        try:
                            article = storage.sql.get(self._sql,
                                                    storage.sql.Article,
                                                    mesg_spec=mesg_spec)
                            log.debug('GOT ARTICLE: %s' % article.mesg_spec)
                        except storage.sql.NotFoundError:
                            pass
                        else:
                            file_rec = article.file
                            self._sql.delete(article)

                            if file_rec and not storage.sql.exists(self._sql, storage.sql.Article.file_id == file_rec.id):
                                log.info('LAST ARTICLE, DELETING FILE: %s' \
                                            % file_rec.subject)
                                report = file_rec.report
                                self._sql.delete(file_rec)

                                if report and not storage.sql.exists(self._sql, storage.sql.File.report_id == report.id):
                                    log.info('LAST FILE, DELETING REPORT: %s' \
                                                % report.subject)
                                    self._sql.delete(report)

                        stats['expired'] += 1
                    else:
                        log.debug('UPDATE TO RDBMS: %s' % mesg_spec)
                        try:
                            art_obj = pickle.loads(art_msg)
                        except pickle.UnpicklingError:
                            log.error(art_msg)
                            raise

                        art_obj['date'] = datetime\
                                        .fromtimestamp(int(art_obj['date']))

                        group_names = art_obj.pop('newsgroups')

                        article, _ = storage.sql.get_or_create(self._sql,
                                                        storage.sql.Article,
                                                        mesg_spec=mesg_spec,
                                                        defaults=art_obj)
                        for k, v in art_obj.items():
                            setattr(article, k, v)

                        for gn in group_names:
                            group = GROUPS.get(gn, None)
                            if not group:
                                group, _ = storage.sql.get_or_create(self._sql,
                                                          storage.sql.Group,
                                                          name=gn)
                                GROUPS[gn] = group
                            if not storage.sql.exists(self._sql, and_(storage.sql.group_articles.c.article_id == article.id, storage.sql.group_articles.c.group_id == group.id)):
                                article.newsgroups.append(group) 
                        stats['updated'] += 1

                    self._sql.flush()
                except: ## some failure, roll back redis and sqa
                    log.error(traceback.format_exc())
                    log.debug('ROLLING BACK: %s, %s' % (set_key, mesg_spec))

                    self._redis.sadd(set_key, mesg_spec)
                    self._redis.set(mesg_spec, art_msg)
                    self._sql.rollback()

            ## END OF BUCKET
            self._sql.commit()        

        return stats

    def group_articles(self, group_name, *args, **kwargs):
        '''
        this little guy produces "File" records comprised of one
        or more "Articles"

        selection/joining strategy directly pilfered from Perl's aub
        invalidate(?):  perhaps after a certain amount of time, if 
                        something remains incomplete we zap  it,
        '''
        ## ensure the /dev/null file, ID 0 exists so we have somewhere to put 
        ## the trash.
        ## TODO: i wish i were kidding but I think SqlAlchemy has a truth
        ## testing bug that prevents you from asserting id = 0 when ID
        ## is the primary key.  (I had to manually create this record)
        null_file, _ = storage.sql.get_or_create(self._sql,
                                    storage.sql.File,
                                    id=0,
                                    subject=u'NULL FILE',
                                    defaults={'from_': u'nobody',
                                              'date': datetime.today(),
                                              'subj_key': u'NULL FILEfrom_'})
        if _: self._sql.commit()
        kill_subject = (
            (lambda x: x == ''),
            (lambda x: x.lower().startswith('re:')),
            (lambda x: '.htm' in x),
            (lambda x: x.count('!') > 1),
        )

        kiss_subject = tuple([re.compile(r, re.I) for r in (
                                    r'^(.*)[^\d](1)/(1)[^\d]', 
                                    r'^(.*\D)(\d+)\s*/\s*(\d+)',
                                    r'^(.*\D)(\d+)\s*\|\s*(\d+)',
                                    r'^(.*\D)(\d+)\s*\\\s*(\d+)',
                                    r'^(.*\D)(\d+)\s*o\s*f\s*(\d+)',
                                    r'^(.*\D)(\d+)\s*f\s*o\s*(\d+)',)])

        subject_hints = tuple([(lambda x: h in x.lower()) for h in [
                                    ".gif", ".jpg", ".jpeg", ".gl",
                                    ".zip", ".au", ".zoo", ".exe",
                                    ".dl", ".snd", ".mpg", ".mpeg",
                                    ".tiff", ".lzh", ".wav", ".iso",
                                    ".mkv", ".bin", ".avi", ".mp3",
                                    ".mp4", ".x264", ".rar" ]])

        stats = {'processed': 0,
                 'updated': 0,
                 'deleted': 0} 

        cq = self._sql.query(storage.sql.Article)\
                            .filter(storage.sql.Article.file_id == None)

        cursor = 0
        chunk_processed = None
        while(chunk_processed != 0):
            chunk_processed = 0
            log.info('Articles (%s, %s)' % (cursor, sql_chunksize))
            for article in cq[cursor:sql_chunksize]:
                try:
                    stats['processed'] += 1
                    chunk_processed += 1

                    subject = article.subject.strip()
                    # tests
                    if any(kill(subject) for kill in kill_subject):
                        log.debug('KILL [%s]' % subject)
                        article.file_id = 0
                        continue

                    name = None
                    part = 1
                    parts = 1
                    for kiss in kiss_subject:
                        match = re.match(kiss, subject)
                        if match:
                            log.debug('KISS [%s]' % subject)
                            name, part, parts = match.groups()
                            part = int(part)
                            parts = int(parts)
                            name = name.strip()
                            ## strip yEnc suffix if obvious..
                            for yesuff in [' yEnc (', ' yEnc']:
                                if name.endswith(yesuff):
                                    name = name[:-len(yesuff)]
                            break

                    if name == None:
                        if any(hint(subject) for hint in subject_hints):
                            log.debug('HINT [%s]' % subject)
                            name = subject
                        else:
                            log.debug('OUT [%s]' % subject)
                            article.file_id = 0
                            continue

                    log.debug('Found <<<%s>>> (%s/%s)' % (name, part, parts))

                    ## update the article w/ its part number
                    if article.part != part:
                        article.part = part

                    ## we have an article we think could possibly be a filepart
                    ## (same subject) and shares a from.  This needs to be cached.
                    try:
                        file_rec = storage.sql.get(self._sql, storage.sql.File,
                                                   subj_key=(name+article.from_))
                    except storage.sql.NotFoundError:
                        file_rec = storage.sql.File(subject=name,
                                                    from_=article.from_,
                                                    subj_key=(name+article.from_),
                                                    date=article.date,
                                                    parts=parts)
                        ## must be in session before we add related items..
                        self._sql.add(file_rec)

                    ## stats
                    mutator = {}

                    if not file_rec.date or file_rec.date < article.date:
                        mutator['date'] = article.date

                    if not file_rec.parts or file_rec.parts < parts:
                        mutator['parts'] = parts

                    ## relations
                    for group in article.newsgroups:
                        #if not file_rec.newsgroups.filter_by(id=group.id).count():
                        if not storage.sql.exists(self._sql, and_(storage.sql.group_files.c.file_id == file_rec.id, storage.sql.group_files.c.group_id == group.id)):
                            file_rec.newsgroups.append(group)

                    if not article.file_id:
                        mutator['bytes_'] = file_rec.bytes_ + article.bytes_
                        article.file = file_rec

                    ## rollup
                    total_parts = file_rec.articles\
                                            .group_by(storage.sql.Article.part).count()
                    if (total_parts >= parts):
                        log.info('File Completion: %s of %s' % (total_parts, parts))
                        mutator['complete'] = True

                    ## apply mutator
                    for k, v in mutator.items():
                        setattr(file_rec, k, v)

                    self._sql.flush()
                    stats['updated'] += 1
                except:
                    log.error(traceback.format_exc())
                    log.debug('ROLLING BACK')

                    self._sql.rollback()

                self._sql.commit()

            cursor += chunk_processed
                        
            self._sql.commit()

        ## back in RDBMS land we can safely loop over all records...
        ## but right now we want processors to run forever, and only die
        ## once the scanners complete.
        #stats['complete'] = True
        return stats

    def group_files(self, group_name, *args, **kwargs):
        stats = {'processed': 0,
                 'updated': 0,
                 'deleted': 0} 

        cq = self._sql.query(storage.sql.File)\
                .filter(storage.sql.File.report_id == None)

        ## the goal of these is to strip out parts of the file names we know
        ## to be representative of the individual files while leaving the
        ## common subject components entact.  Some posts specify files parts of
        ## a post, some specify file size of file.  Many provide none of this 
        ## info, so for now we're ignoring all of it.

        ## TODO: possibly use the filepart data when available to more fully 
        ## populate the auto-Report
        kiss_subject = tuple([re.compile(r, re.I) for r in (
            r'''(.*)\s*<\s*\d+\s*/\s*\d+\s*\(.*\)\s*>\s*<\s*.*\s*>\s*(.*)''',
            r'''(.*)\s*\[\s*\d+\s*(?:/|of)\s*\d+\s*\]\s*(.*)''',
            r'''(.*)(?:\.part\d+|\.vol\d+\+\d+)\s*(.*)''',
            r'''(.*)(?:\.[a-z0-9_]{2,4})+\s*(.*)''',
            r'''(.*)(?:[\.\-]sample|sample[\.\-])(.*)''',
        )])

        cursor = 0
        chunk_processed = None
        while(chunk_processed != 0):
            chunk_processed = 0
            log.info('Files (%s, %s)' % (cursor, sql_chunksize))
            for file_rec in cq[cursor:sql_chunksize]:
                stats['processed'] += 1
                try:
                    if not file_rec.articles.count():
                        log.info('Deleting empty file %s' % file_rec.subject)
                        self._sql.delete(file_rec)
                        stats['deleted'] += 1
                        continue

                    ## look for x of y patterns in subject + parse out
                    ## also look at file extension, if subject matches  and has
                    ## a .r(ar|\d) extension
                    subject = file_rec.subject
                    kissed = False
                    for kiss in kiss_subject:
                        match = re.match(kiss, subject)
                        if match:
                            kissed = True    
                            subject = u''.join(match.groups(u''))

                    ##max len unique blah blah
                    subject = subject[:255]

                    if not kissed:
                        log.error("Couldn't parse %s" % file_rec.subject)
                        raise Exception
                    else:
                        log.debug("Parsed: %s" % subject)
                    ## get or create report with cleaned article subject
                    report, _ = storage.sql.get_or_create(self._sql,
                                                        storage.sql.Report,
                                                        subject=subject)

                    mutator = {}
                    ## asscociate file with report if not already linked
                    ## relations
                    for group in file_rec.newsgroups:
                        if not storage.sql.exists(self._sql, and_(storage.sql.group_reports.c.report_id == report.id, storage.sql.group_reports.c.group_id == group.id)):
                            report.newsgroups.append(group)

                    if not file_rec.report_id:
                        mutator['bytes_'] = report.bytes_ + file_rec.bytes_
                        mutator['report'] = report

                    for k, v in mutator.items():
                        setattr(report, k, v)

                    log.debug('Report << %s >>' % subject)

                    self._sql.flush()
                except:
                    log.error(traceback.format_exc())
                    log.debug('ROLLING BACK')

                    self._sql.rollback()

                self._sql.commit()

            cursor += chunk_processed

            self._sql.commit()

        return stats

