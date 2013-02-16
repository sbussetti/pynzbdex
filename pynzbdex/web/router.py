import os
import re
import sys
import time
import datetime
from SocketServer import ThreadingMixIn
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
import urlparse
import traceback
from collections import OrderedDict
import logging
import copy
import urllib
import math

from pynzbdex import settings
from pynzbdex.web import resolver
from pynzbdex.web.views import (Home, SearchGroups, Search, ViewArticle,
                                ViewFile, MakeNZB, ViewReport, SearchRoute)


log = logging.getLogger(__name__)


URL_BASE = '/'

ROUTES = (
            (r'^$',
                Home().dispatch,
                'home'),
            (r'^search/$',
                SearchRoute().dispatch,
                'search_route'),
            (r'^search/(?P<doctype>\w+)/$',
                Search().dispatch,
                'search'),
            (r'^search/(?P<doctype>\w+)/(?P<group_name>.+)/$',
                Search().dispatch,
                'search_group'),
            (r'^search/(?P<group_name>.+)/$',
                SearchGroups().dispatch,
                'search_groups'),
            (r'^view/article/(?P<mesg_id>.+)/$',
                ViewArticle().dispatch,
                'article_view'),
            (r'^view/file/(?P<id>\d+)/$',
                ViewFile().dispatch,
                'file_view'),
            (r'^view/report/(?P<id>\d+)/$',
                ViewReport().dispatch,
                'report_view'),
            (r'^nzb/$',
                MakeNZB().dispatch,
                'make_nzb'),
        )

class Router(object):
    def __init__(self, routes, url_base):
        self.routes = routes
        self.url_base = url_base

    def url_reverse(self, view_name, *args, **kwargs):
        matches = []
        for route, view, name in self.routes:
            if view_name == name:
                res = resolver.normalize(route)
                ## res looks something like
                ## (u'article/view/%(mesg_id)s/', ['mesg_id'])
                ## or
                ## (u'article/view/%(_0)s/', ['_0'])
                matches.extend(res)

        for patt, fields in matches:
            log.debug(['REV', patt, fields])
            path = None
            if not fields:
                path = patt
            else:
                if kwargs:
                    repl = dict(kwargs)
                else:
                    repl = dict(zip(fields, args))
                path = patt % repl
            log.debug(['REV', path])
            return '%s%s' % (self.url_base, path)

        raise Exception('No view named %s' % view_name)

    def url_forward(self, path):
        for route, view, name in self.routes:
            rm = re.compile(route, re.I)
            # this is a bug because l strip does al ist of chars not string
            p = path.lstrip(self.url_base)
            m = rm.search(p)
            if m:
                kwargs = m.groupdict()
                if kwargs:
                    kwargs = {k: urllib.unquote(v) for k, v in kwargs.items() if v != None}
                    args = ()
                else:
                    args = [urllib.unquote(v) for v in m.groups() if v is not None]
                    kwargs = {}
                
                log.info(['FWD', view, args, kwargs])
                return (view, args, kwargs)
        return (None, None, None)
        
        
router = Router(ROUTES, URL_BASE)  
      
