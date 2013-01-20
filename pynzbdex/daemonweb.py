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

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import DeferredReflection

from pynzbdex import storage, daemonweb_regex_helper as resolver, settings
from pynzbdex.router import router
from pynzbdex.template import templates

storage.riak.BACKEND = 'HTTP'




logging.basicConfig(format='%(levelname)s: %(message)s')
log = logging.getLogger(__name__)
log.setLevel('DEBUG')

logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)


##################
#
#   HELPERS
#
##################


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


class PyNZBDexHandler(BaseHTTPRequestHandler):
    server_version = 'PyNZBDex/1.0 HTTP/1.1'

    def do_GET(self):
        req = self.get_request()

        view, args, kwargs = router.url_forward(req.path)
        
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
    

class MultiThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    pass                



if __name__ == '__main__':
    HandlerClass = PyNZBDexHandler
    ServerClass  = MultiThreadedHTTPServer
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
