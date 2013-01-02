import os
import re
import sys
import time
import datetime
import BaseHTTPServer
import urlparse
import traceback
from collections import OrderedDict
import logging
import copy
import urllib

from jinja2 import Template, Environment, PackageLoader

from pynzbdex import storage, daemonweb_regex_helper as resolver
storage.BACKEND = 'HTTP'



logging.basicConfig(format='%(levelname)s: %(message)s')
log = logging.getLogger(__name__)
log.setLevel('DEBUG')


##################
#
#   HELPERS
#
##################
URL_BASE = '/'

def url_reverse(view_name, *args, **kwargs):
    matches = []
    for route, view, name in ROUTES:
        if view_name == name:
            res = resolver.normalize(route)
            ## res looks something like
            ## (u'article/view/%(mesg_id)s/', ['mesg_id'])
            ## or
            ## (u'article/view/%(_0)s/', ['_0'])
            matches.extend(res)

    for patt, fields in matches:
        path = None
        if not fields:
            path = patt
        else:
            if kwargs:
                repl = dict(kwargs)
            else:
                repl = dict(zip(fields, args))
            path = patt % repl

        return '%s%s' % (URL_BASE, path)

    raise Exception('No view named %s' % view_name)

def url_forward(path, base=URL_BASE):
    for route, view, name in ROUTES:
        rm = re.compile(route, re.I)
        p = path.lstrip('/')
        m = rm.search(p)
        if m:
            kwargs = m.groupdict()
            if kwargs:
                kwargs = {k: urllib.unquote(v) for k, v in kwargs.items()}
                args = ()
            else:
                args = [urllib.unquote(v) for v in m.groups()]
                kwargs = {}
            
            log.info([view, args, kwargs])
            return (view, args, kwargs)
    return (None, None, None)

class PagedResults(object):
    def __init__(self, query, sort, page, per_page):
        if per_page > 100:
            per_page = 100
        offset = (page - 1) * per_page

        q = storage.Article.solrSearch(query, start=offset,
                                       rows=per_page, sort=sort, wt='xml')

        total = q.length()
        prev_offset = offset - per_page
        next_offset = offset + per_page
        current_offset = offset + 1
        last_page = int(total / per_page) + 1
        current_page = int(offset / per_page) + 1
        pages = []

        if prev_offset < 0:
            prev_offset = None
        else:
            prev_page = int(prev_offset / per_page) + 1
        if next_offset > total:
            next_offset = None
        else:
            next_page = int(next_offset / per_page) + 1
        rem = (total % per_page)
        if rem:
            last_offset = total - rem
        else:
            last_offset = total - per_page

        ew = current_page + 5
        if ew > last_page:
            ew = last_page + 1
        cw = current_page - 3
        if cw <  1:
            ew += -1 * cw
            cw = 1
        for i in range(cw, ew):
            pages.append((i, i * per_page))

        self.q = q
        self.per_page = per_page
        self.offset = offset
        self.total = total
        self.prev_offset = prev_offset
        self.next_offset = next_offset
        self.current_offset = current_offset
        self.last_page = last_page
        self.current_page = current_page
        self.pages = pages

    def all(self):
        return self.q.all()


class ImmutableObject(object):
    _io_locked = False

    def __init__(self, **kwargs):
        self._io_attrs = kwargs
        self._io_locked = True

    def __getattr__(self, k):
        ## TODO: make child objects gain
        ## immutability by mutation
        if k.startswith('_io_'):
            return super(ImmutableObject, self).__getattr__(k)
        else:
            return copy.deepcopy(self._io_attrs.get(k, None))

    def __setattr__(self, k, v):
        if self._io_locked:
            raise TypeError('Request is read only')
        else:
            super(ImmutableObject, self).__setattr__(k, v)


class PyNZBRequest(ImmutableObject):
    pass


###########
#
#   VIEWS
#
#################

class PyNZBDexViewsBase(object):
    def dispatch(self, request, *args, **kwargs):
        methods = ['GET', 'POST']

        if request.method in methods: 
            func = getattr(self, request.method.lower(), None)
            if func:
                return func(request, *args, **kwargs)
            else:
                raise Exception('Unhandled method: %s' % request.method)
        else:
            raise Exception('Unknown method: %s' % request.method)

    def render_template(self, name, ctx={}):
        tmpl = templates.get_template(name)
        return tmpl.render(**ctx)

    def render(self, ctx={}):
        return self.render_template(self.template_name, ctx)
        

class PyNZBDexHome(PyNZBDexViewsBase):
    template_name = 'index.html'

    def dispatch(self, request, **kwargs):
        return self.render()


class PyNZBDexSearchGroup(PyNZBDexViewsBase):
    template_name = 'search_group.html'

    def get(self, request, *args, **kwargs):
        query = request.GET.get('q', None)
        sort = request.GET.get('s', 'date desc')
        page = int(request.GET.get('p', 1) or 1)
        per_page = int(request.GET.get('pp', 25) or 25)

        pager = PagedResults(query, sort, page, per_page)

        return self.render({'results': pager.all(),
                            'pager': pager,
                            'today': datetime.datetime.today(),
                            'group': group,
                            'request': request,  })


class PyNZBDexSearchArticle(PyNZBDexViewsBase):
    template_name = 'search_article.html'

    def get(self, request, group_name, *args, **kwargs):
        query = request.GET.get('q', None)
        sort = request.GET.get('s', 'date desc')
        page = int(request.GET.get('p', 1) or 1)
        per_page = int(request.GET.get('pp', 25) or 25)

        cq = "newsgroups:'%s'" % group_name
        if query:
            cq = '%s AND %s' % (cq, query)

        group = storage.Group.get(group_name)
        pager = PagedResults(cq, sort, page, per_page)

        return self.render({'results': pager.all(),
                            'pager': pager,
                            'today': datetime.datetime.today(),
                            'group': group,
                            'request': request,  })


class PyNZBDexViewArticle(PyNZBDexViewsBase):
    template_name = 'view_article.html'

    def get(self, request, mesg_id, *args, **kwargs):
        delete = request.GET.get('delete', False)

        article = storage.Article.get(mesg_id)

        if delete:
            article.delete()

        return self.render({'request': request,
                            'article': article})
        

######################
##
#   TEMPLATE FILTERS
##
#####
def date(value, fmt='%d %b %Y'):
    return value.strftime(fmt)

def age(date, units='days'):
    return getattr((datetime.datetime.today() - date), units, None)

def human_size(num):
    for x in ['bytes','KB','MB','GB']:
        if num < 1024.0:
            return "%3.1f%s" % (num, x)
        num /= 1024.0
    return "%3.1f%s" % (num, 'TB')

def query(request, **kwargs):
    base = request.GET
    base.update(kwargs)
    return urllib.urlencode(base)

def keys(data):
    return data.keys()

def url(name, *args, **kwargs):
    return url_reverse(name, *args, **kwargs)

templates = Environment(loader=PackageLoader('pynzbdex', 'templates'))
templates.filters.update({
        'date': date,
        'age': age,
        'human_size': human_size,
        'query': query,
        'keys': keys,
        'url': url,
    })

ROUTES = (
            ('^$', PyNZBDexHome().dispatch, 'home'),
            ('^article/search/(?P<group_name>.+)/$',
                PyNZBDexSearchArticle().dispatch,
                'article_search'),
            ('^article/view/(?P<mesg_id>.+)/$',
                PyNZBDexViewArticle().dispatch,
                'article_view'),
        )

class PyNZBDexHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    server_version = 'PyNZBDex/1.0 HTTP/1.1'

    def do_GET(self):
        req = self.get_request()

        view, args, kwargs = url_forward(req.path)
        
        resp, code = (None, 404)
        if view:
            try:
                resp = view(req, *args, **kwargs)
                code = 200
            except:
                resp = traceback.format_exc()
                code = 500
        self.send(resp, code)

    def do_POST(self):
        return self.do_GET()

    def do_HEAD(self):
        self.send_head()

    def send_head(self, resp=200, size=0):
        self.send_response(resp)
        self.send_header("Content-type", 'text-html')
        self.send_header("Content-Length", size)
        self.send_header("Last-Modified", self.date_time_string(time.time()))
        self.end_headers()

    def send(self, body, resp=200):
        if body:
            b = body.encode('utf-8')
            size = len(b)
        else:
            b = u''
            size = 0
        self.send_head(resp, size)
        if size:
            self.wfile.write(b)
        return size

    def get_request(self):
        ## parse path..
        p = urlparse.urlparse(self.path)
        q_get = OrderedDict(urlparse.parse_qs(p.query))
        ## flatten..
        for k, v in q_get.items():
            if len(v) == 1:
                q_get[k] = v[0]
            
        q_post = OrderedDict({})
        q_request = OrderedDict(q_get.items() + q_post.items())
        return PyNZBRequest(
                    client_address=self.client_address,
                    server=self.server,
                    method=self.command,
                    headers=self.headers,
                    path=p.path,
                    ## OrderedDict..
                    GET=q_get,
                    POST=q_post,
                    REQUEST=q_request,
                )
                    


if __name__ == '__main__':
    HandlerClass = PyNZBDexHandler
    ServerClass  = BaseHTTPServer.HTTPServer
    Protocol     = "HTTP/1.1"

    if sys.argv[1:]:
        port = int(sys.argv[1])
    else:
        port = 6392

    if sys.argv[2:]:
        host = int(sys.argv[2])
    else:
        host = '0.0.0.0'
    server_address = (host, port)

    HandlerClass.protocol_version = Protocol
    httpd = ServerClass(server_address, HandlerClass)

    sa = httpd.socket.getsockname()
    print "Serving HTTP on", sa[0], "port", sa[1], "..."
    httpd.serve_forever()