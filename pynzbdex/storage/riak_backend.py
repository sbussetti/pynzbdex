import datetime
import time
import pytz

from riakkit import *
import riak


BACKEND = 'HTTP'
#BACKEND = 'PBC'

class ClientWrapper(object):
    _cw_client = None
    _cw_backend = None

    def _cw_client_(self):
        ## this might be hacky, but it does stuff to the connection
        ## during the metaclass setup, which sucks.
        if self._cw_client is None or self._cw_backend != BACKEND:
            if BACKEND == 'PBC':
                self._cw_client = riak.RiakClient(port=8087, transport_class=riak.RiakPbcTransport)
            else:
                self._cw_client = riak.RiakClient()
            self._cw_backend = BACKEND
        return self._cw_client

    def __getattr__(self, k):
        if k.startswith('_cw_'):
            super(ClientWrapper, self).__getattr__(k)
        else:
            return getattr(self._cw_client_(), k)

    def __setattr__(self, k, v):
        if k.startswith('_cw_'):
            super(ClientWrapper, self).__setattr__(k, v)
        else:
            setattr(self._cw_client_(), k, v)

client = ClientWrapper()
epoch_datetime_utc = datetime.datetime.fromtimestamp(time.mktime(time.gmtime(0)))

class DocBase(Document):
    client = client

    def __str__(self):
        return unicode(self).encode('utf-8')


class Group(DocBase):
    bucket_name = 'group'

    ## NNTP metadata
    # groupname is the key.. and is unique
    #group = StringProperty(required=True, unique=True)
    count = IntegerProperty(default=0)
    first = IntegerProperty(default=0)
    ## could be none
    last = IntegerProperty()
    flag = StringProperty()

    ## THIS NAMING IS BAD...
    ## metaprop for resumption (ID of the last article we processed)
    last_stored = IntegerProperty()

    ## audit/control
    active = BooleanProperty(default=False)
    ## riakkit automatically defaults to now, left alone 
    last_indexed = DateTimeProperty(default=None)

    def __unicode__(self):
        return u'name(%s) count(%s) first(%s) last(%s) flag(%s) last_stored(%s)' \
                    % (self.key, self.count, self.first, self.last, self.flag, self.last_stored)


class Article(DocBase):
    bucket_name = 'article'

    ## CONTROL METADATA
    #__short_headers = BooleanProperty(default=True)
    # short headers are required so this is implicit
    nn_long_headers = BooleanProperty(default=False)
    nn_body = BooleanProperty(default=False)

    ## SHORT HEADERS
    ## implicitly this is the key.
    #message_id = StringProperty(required=True, unique=True)
    #article_num = IntegerProperty()

    lines = IntegerProperty(default=0)
    bytes_ = IntegerProperty(default=0)
    ## this is a combo of group and article num, could be many groups..
    ## takes format of: ## <nntp servername> <groupname>:<article_num>
    ## on import xref will be converted into a series of dynamic fields
    ## all prefixed with 'xref_'
    xref = StringProperty()
    from_ = StringProperty(required=True)
    subject = StringProperty(required=True)
    ## we can't set a null date so make everything default in mem to the epoch.
    date = DateTimeProperty(required=True, default=epoch_datetime_utc)
    references = StringProperty()

    ## special -- this comes from longheaders, BUT, we also manually populate
    ## it as we scrape individual groups
    newsgroups = ListProperty(required=True)

    ## ADDL FIELDS FROM LONG HEADERS
    ## internally Riakkit uses sets on this so we don't need to worry
    ## about deduping.  This is more important than setting up references
    ## back to the Group documents, imo, since we can look those up based
    ## on each element of this list.  The inverse is more difficult -- 
    ## getting all articles for a given group, since articles can be
    ## crossposted... so this has to be done via the Solr-like search based on
    ## the xref_message_ids tags.

    content_transfer_encoding = StringProperty()
    content_type = StringProperty()
    mime_version = StringProperty()
    nntp_posting_date = StringProperty()
    nntp_posting_host = StringProperty()
    organization = StringProperty()
    path = StringProperty()
    reply_to = StringProperty()
    sender = StringProperty()
    user_agent = StringProperty()
    ## X-prefixed fields are variable..... try and keep on top of
    ## all of them, BUT, probably store them anyway?
    x_abuse_and_dmca_info = StringProperty()
    x_complaints_info = StringProperty()
    x_complaints_to = StringProperty()
    x_dmca_complaints_to = StringProperty()
    x_dmca_notifications = StringProperty()
    x_forwarded = StringProperty()
    x_newsposter = StringProperty()
    x_newsreader = StringProperty()
    x_no_archive = StringProperty()
    x_received_bytes = StringProperty()
    x_original_bytes = StringProperty()
    x_postfilter = StringProperty()
    x_comments = StringProperty()
    x_trace = StringProperty()
    x_usenet_provider = StringProperty()
    
    def __unicode__(self):
        return 'message_id(%s) subject(%s)' % (self.key, self.subject)

