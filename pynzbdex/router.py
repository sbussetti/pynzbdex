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

from pynzbdex import daemonweb_regex_helper as resolver, settings
from pynzbdex.views import (PyNZBDexHome, PyNZBDexSearchGroup, PyNZBDexSearch,
                            PyNZBDexViewArticle, PyNZBDexViewFile,
                            PyNZBDexMakeNZB, PyNZBDexViewReport)


log = logging.getLogger(__name__)


URL_BASE = '/'

ROUTES = (
            ('^$', PyNZBDexHome().dispatch, 'home'),
            ('^search/(?P<doctype>.+)/(?P<group_name>.+)/$',
                PyNZBDexSearch().dispatch,
                'search'),
            ('^view/article/(?P<mesg_id>.+)/$',
                PyNZBDexViewArticle().dispatch,
                'article_view'),
            ('^view/file/(?P<id>.+)/$',
                PyNZBDexViewFile().dispatch,
                'file_view'),
            ('^view/report/(?P<id>.+)/$',
                PyNZBDexViewReport().dispatch,
                'report_view'),
            ('^nzb/$',
                PyNZBDexMakeNZB().dispatch,
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
            path = None
            if not fields:
                path = patt
            else:
                if kwargs:
                    repl = dict(kwargs)
                else:
                    repl = dict(zip(fields, args))
                path = patt % repl
    
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
                    kwargs = {k: urllib.unquote(v) for k, v in kwargs.items()}
                    args = ()
                else:
                    args = [urllib.unquote(v) for v in m.groups()]
                    kwargs = {}
                
                log.info([view, args, kwargs])
                return (view, args, kwargs)
        return (None, None, None)
        
        
router = Router(ROUTES, URL_BASE)  
      
