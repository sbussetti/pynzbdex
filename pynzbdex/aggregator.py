from datetime import datetime
import time
import logging
import sys
import pytz
import traceback
import re

import riak
import dateutil.parser
import iso8601
from redis import StrictRedis
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import DeferredReflection
from sqlalchemy.sql import expression

from pynzbdex.pynntpcli import NNTPProxyClient
from pynzbdex import storage, settings
storage.riak.BACKEND = 'PBC'


logging.basicConfig(format='%(levelname)s: %(message)s')
log = logging.getLogger(__name__)
log.setLevel('INFO')


## TODO: move these to settings
redis_indexbuckets = 1000
nntp_chunksize = 1000
riak_chunksize = 1000
# when querying ranges, the max we'll span
# (anything larger needs to be divided and aggregated)
range_stepsize = 10000

class FieldParsers(object):
    ## a collection of classmethods to handle complex field normalization.
    ## this might get offloaded into the `storage` module..
    @classmethod
    def commalist(kls, value):
        if value:
            return value.split(',')
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
            self.__nntp = NNTPProxyClient(host=nntp_cfg['HOST'],
                                          port=nntp_cfg['PORT'])
        return self.__nntp

    @property
    def _redis(self):
        if not self.__redis:
            redis_cfg = settings.REDIS['default']
            self.__redis = StrictRedis(host=redis_cfg['HOST'],
                                       port=redis_cfg['PORT'],
                                       db=redis_cfg['DB'])
        return self.__redis

    @property
    def _sql(self):
        if not self.__sql:
            sql_cfg = settings.DATABASE['default']
            dsn = '%(DIALECT)s+%(DRIVER)s://%(USER)s:%(PASS)s@%(HOST)s:%(PORT)s/%(NAME)s' % sql_cfg
            sql_engine = create_engine(dsn)
            storage.sql.Base.metadata.create_all(sql_engine)
            DeferredReflection.prepare(sql_engine)
            Session = sessionmaker(bind=sql_engine)
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
        msg = json.dumps({
                'from_': article_d.from_,
                'bytes_': article_d.bytes_,
                'subject': article_d.subject,
                'date': time.mktime(article_d.date.timetuple()),
                'newsgroups': article_d.newsgroups,
            })
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

    def invalidate_groups(self, prefix='alt.binaries.*'):
        ## scans the groups we know and prunes ones that do not exist
        ## grouplist is not prohibitvely large as article list,
        ## so we can just do it like this.... article list will take
        ## some different tactics... but is actually easier due to first/last
        ## article ID..

        ## so this is just a list of groupnames from the NNTP server
        groups = self._nntp.get_groups(prefix)
        nntp_groups = [g['group'] for g in groups]

        log.info('Invalidating groups, got %s groups from NNTP server' \
                    % len(nntp_groups))
        ## mr object already filters based on the prefix for the groups
        ## we want to invalidate. 
        q = storage.riak.Group.mapreduce()
        if prefix[-1] == '*': ## startswith
            p = prefix.rstrip('*')
            q.add_key_filters(riak.key_filter.starts_with(p))
        else:
            q.add_key_filters(riak.key_filter.eq(prefix))

        #q.map("""function(v){ return [[v.bucket, v.key, v.vclock]]; }""")
        doc_groups = [d.get_key() for d in q.run()]
        dead_groups = set(doc_groups) - set(nntp_groups)

        ##TODO: we have to get the keylist outside of riak  .. is there
        ## a way to feed back in the keylist and have things deleted
        ## via the reduce-phase deleted?
        keys = []
        for k in dead_groups:
            log.info('Deleting group: %s' % k)
            fro = FakeRiakObject(FakeRiakBucket(storage.riak.Group.bucket_name), k, None)
            storage.riak.client.get_transport().delete(fro)
            keys.append(k)
        return keys

    def scrape_groups(self, prefix='alt.binaries.*'):
        ## updates our store of groups based on a prefix.
        log.info('Updating group store')
        groups = self._nntp.get_groups(prefix)
        for group in groups:
            log.debug('Updating group: %s' % group['group'])
            group_d = storage.riak.Group.getOrNew(group['group'],
                                            **{'first': group['first'],
                                                'last': group['last'],
                                                'flag': group['flag']})
            ## group list doesn't return count.. only the get 
            ## single group groups style does...
            group_d.save()

    def scrape_articles(self, group_name, get_long_headers=False,
                        invalidate=False, cached=True, offset=0,
                        resume=False, max_processed=None):
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

        first = (group_d.last_stored if resume 
                                        and group_d.last_stored >= group_d.last
                                     else group_d.first or 0)
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
        while ((cursor_f < last)
                    and
                (   max_processed == None
                        or 
                    (max_processed != None and processed_articles < max_processed))):
            cursor_l = cursor_f + nntp_chunksize - 1
            if cursor_l > group_d.last:
                cursor_l = group_d.last

            log.debug('Get articles for %s between (%s, %s)' \
                        % (group_name, cursor_f, cursor_l))
            articles = self._nntp.get_group((cursor_f, cursor_l), group_name)

            ## increment..
            cursor_f += nntp_chunksize
            ## next, if not in resume mode,  look for
            ## noncontiguous missing articles, for instance ones
            ## that have been specifically taken down, while we're in here
            ## updating others.
            previous_article_num = None
            for article_num, short_headers in articles:
                if (    max_processed != None
                        and processed_articles >= max_processed):
                    break
                log.debug('Processing article: %s:%s' % (group_name,
                                                         article_num))
                processed_articles += 1
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

                previous_article_num = article_num

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
                has_bad_xref_tag = False

                xref_bits = [a.strip().split(':', 1)
                                for a in short_headers['xref'].split(' ')][1:]
                for g, i in xref_bits:
                    if hasattr(article_d, 'xref_%s_int' % g):
                        has_bad_xref_tag = True

                    ## use int suffix to tie into default schema..
                    k = 'xref_%s_int' % g.replace('.', '_')
                    norm_headers[k] = int(i)
                    norm_headers['newsgroups'].append(g)

                if has_bad_xref_tag:
                    log.error('>>>>>>> HAS BAD XREF TAG <<<<<<<')
                    article_d.delete()
                    article_d = storage.riak.Article(key)
                else:
                    log.debug('<<<<<<< XREF TAGS CLEAN >>>>>>>>')

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
                for i in range(0, 5):
                    try:
                        saved = article_d.save()
                    except Exception, e:
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
            group_d.reload()
            #if (    previous_article_num and
            #        (abs(group_d.last_stored - previous_article_num)
            #            <= nntp_chunksize)  ):
            # yes you will concievably end up with gapping, but the
            # non-resuming scanners will fill it in..
            if previous_article_num:
                group_d.last_stored = previous_article_num
                log.info('LAST STORED: %s' % group_d.last_stored)
                group_d.save()

        return {    'updated': updated_articles,
                    'deleted': deleted_articles,
                    'processed': processed_articles,
                    'last_message_id': previous_article_num, }

    def process_redis_cache(self, *args, **kwargs):
        ## this reads the redis cache and stores the resulting
        ## message buckets to the RDBMS -- it's decoupled
        ## through redis with the assumption that this will
        ## be run from a separate thread or process than
        ## the article scraper.

        log.info('Processing Redis Cache')
        ## fuck you sqlalchemy.  So Session is fucking code for secret
        ## transaction?  Seriously fuck youuuu.
        GROUPS = {}
        PROCESSED_MESSAGES = {}

        total_processed = 0
        total_updated = 0
        total_expired = 0
        for b in xrange(0, redis_indexbuckets):
            set_key = 'newart:%s' % b
            log.debug('Processing Redis Index Bucket: %s' % set_key)
            try:
                for mesg_spec in self._redis.smembers(set_key):
                    art_msg = self._redis.get(mesg_spec)
                    ## our sets persist longer than our keys
                    if art_msg:
                        if art_msg.startswith('EXPIRE'):
                            log.debug('EXPIRED FROM RDBMS: %s' % mesg_spec)
                            ## this should check if the File (if any) associated
                            ## with this thing still has any other assocated records
                            ## and delete it if not
                            deleted_article = storage.sql.get_and_delete(self._sql,
                                                        storage.sql.Article,
                                                        mesg_spec=mesg_spec)

                            if deleted_article:
                                file_rec = deleted_article.file
                                if file_rec.articles.count() == 0:
                                    log.info('LAST ARTICLE, DELETING FILE: %s' % file.subject)
                                    self._sql.delete(file_rec)

                            total_expired += 1
                        else:
                            log.debug('UPDATED TO RDBMS: %s' % mesg_spec)
                            art_obj = json.loads(art_msg)
                            ##temp (from -> from_)
                            art_obj['date'] = datetime.fromtimestamp(int(art_obj['date']))
                            group_names = art_obj.pop('newsgroups')

                            article = storage.sql.get_or_create(self._sql,
                                                                storage.sql.Article,
                                                                mesg_spec=mesg_spec,
                                                                defaults=art_obj)
                            for k, v in art_obj.items():
                                setattr(article, k, v)
                            #article.update(art_obj)

                            for group_name in group_names:
                                group = GROUPS.get(group_name, None)
                                if not group:
                                    group = storage.sql.get_or_create(self._sql,
                                                                      storage.sql.Group,
                                                                      name=group_name)
                                    GROUPS[group_name] = group
                                if not article.newsgroups.filter_by(id=group.id).count():
                                    article.newsgroups.append(group) 
                            total_updated += 1

                        try:
                            PROCESSED_MESSAGES[set_key].append(mesg_spec)
                        except KeyError:
                            PROCESSED_MESSAGES[set_key] = [mesg_spec, ]

                    else:
                        ## clean up my mistakes
                        self._redis.srem(set_key, mesg_spec)

                    total_processed += 1
                    if not (total_processed % 100):
                        ## flush every 100 records
                        for set_key, specs in PROCESSED_MESSAGES.iteritems():
                            for mesg_spec in specs:
                                self._redis.srem(set_key, mesg_spec)
                                self._redis.delete(mesg_spec)
                        ## flush at the end of every bucket.
                        log.debug('COMMIT TO RDBMS')
                        self._sql.commit()
            except:
                log.error(traceback.format_exc())
                ## SQA Session rollsback here
            else:
                ## SQA rolls back, like a dick, so only clear
                ## redis if we got out cleanly
                for set_key, specs in PROCESSED_MESSAGES.iteritems():
                    for mesg_spec in specs:
                        self._redis.srem(set_key, mesg_spec)
                        self._redis.delete(mesg_spec)
                ## flush at the end of every bucket.
                log.debug('COMMIT TO RDBMS')
                self._sql.commit()
                ## once I understand redis piplining this can be further
                ## optimized
        return {'processed': total_processed,
                'updated': total_updated,
                'deleted': total_expired}

    def group_articles(self, group_name, full_scan=False, complete_only=False, *args, **kwargs):
        '''
        this little guy produces "File" records comprised of one
        or more "Articles"

        selection/joining strategy directly pilfered from Perl's aub
        full_scan:      look at all articles not just those not associated
                        with a file
        complete_only:  only attempt to raise completion on existing,
                        incomplete Files
        invalidate(?):  perhaps after a certain amount of time, if 
                        something remains incomplete we zap  it,
        '''
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

        offset = 0
        limit = 5000

        ## this needs to be further filtered to only
        ## include articles not already associated with a file.
        cq = self._sql.query(storage.sql.Article)
        if full_scan:
            cq = cq.filter(storage.sql.Article.newsgroups.any(name=group_name))
        else:
            cq = cq.filter(storage.sql.Article.newsgroups.any(name=group_name),
                           storage.sql.Article.file_id == None)
        cq = cq.order_by(expression.asc('subject'))

        total = cq.count()
        while offset < total:
            log.info('Articles: %s - %s' % (offset, (offset+limit)))
            for article in cq[offset:(offset+limit)]:
                offset += 1
                stats['processed'] += 1

                subject = article.subject.strip()
                # tests
                if any(kill(subject) for kill in kill_subject):
                    log.debug('KILL [%s]' % subject)
                    continue

                name = None
                part = 1
                parts = 1
                for kiss in kiss_subject:
                    match = re.match(kiss, subject)
                    if match:
                        log.debug('KISS [%s]' % subject)
                        name, part, parts = match.groups()
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
                        continue

                log.debug('Found <<<%s>>> (%s/%s)' % (name, part, parts))

                ## update the article w/ its part number
                if article.part != part:
                    article.part = part
                    self._sql.add(article)

                ## we have an article we think could possibly be a filepart
                ## e.g. has at least one group in common,
                ## and shares a from.  This needs to be cached.
                try:
                    file_rec = storage.sql.get(self._sql, storage.sql.File,
                                storage.sql.File.newsgroups.any(storage.sql.Group.id.in_([g.id for g in article.newsgroups])),
                                from_=article.from_, subject=name)
                except storage.sql.NotFoundError:
                    file_rec = storage.sql.File(subject=name,
                                                from_=article.from_,
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

                if not file_rec.articles.filter_by(id=article.id).count():
                    mutator['bytes'] = file_rec.bytes_ + article.bytes_
                    mutator['article'] = article

                ## relations
                for group in article.newsgroups:
                    if not file_rec.newsgroups.filter_by(id=group.id).count():
                        file_rec.newsgroups.append(group)

                ## rollup
                if file_rec.articles.count() == parts:
                    mutator['complete'] = True

                #file_rec.update(mutator)
                for k, v in mutator.items():
                    setattr(file_rec, k, v)

                #flush every 100 records
                if not (offset % 100):
                    self._sql.commit()

                stats['updated'] += 1
                    
            self._sql.commit()

        return stats

