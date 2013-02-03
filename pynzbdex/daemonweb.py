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
import urllib
import math

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import DeferredReflection
from setproctitle import setproctitle

from pynzbdex import storage, daemonweb_regex_helper as resolver, settings
from pynzbdex.router import router
from pynzbdex.template import templates
from pynzbdex import daemonweb_http as http

storage.riak.BACKEND = 'HTTP'

PROC_TITLE_BASE = os.path.basename(__file__)

logging.basicConfig(format='%(levelname)s:(%(name)s.%(funcName)s) %(message)s')
log = logging.getLogger(__name__)
log.setLevel('DEBUG')


class PyNZBDexHandler(BaseHTTPRequestHandler):
    server_version = 'PyNZBDex/1.0 HTTP/1.1'

    def get_response(self, post_data=''):
        req = self.get_request(post_data)

        view, args, kwargs = router.url_forward(req.path)
        
        resp = http.PyNZBResponse(status_code=404)
        if view:
            try:
                resp = view(req, *args, **kwargs)
            except:
                resp = http.PyNZBResponse(body=traceback.format_exc(),
                                          status_code=500,
                                          content_type='text/plain')
        log.debug([resp.status_code, resp.content_type])
        self.send(resp.body, resp.status_code, resp.content_type, resp.headers)

    def do_GET(self):
        log.debug('do_GET')
        self.get_response()

    def do_POST(self):
        log.debug('do_POST')
        length = int(self.headers.getheader('content-length'))        
        post_data = self.rfile.read(length)
        return self.get_response(post_data=post_data)

    def do_HEAD(self):
        log.debug('do_HEAD')
        self.send_head()

    def send_head(self, resp=200, size=0, content_type='text/html', headers={}):
        self.send_response(resp)
        self.send_header("Content-type", '%s; charset=utf-8' % content_type)
        self.send_header("Content-Length", size)
        self.send_header("Last-Modified", self.date_time_string(time.time()))
        for k, v in headers.items():
            self.send_header(k, v)
        self.end_headers()

    def send(self, body, resp=200, content_type='text/html', headers={}):
        if body:
            b = body.encode('utf-8')
            size = len(b)
        else:
            b = u''.encode('utf-8')
            size = 0
        self.send_head(resp, size, content_type, headers)
        if size:
            self.wfile.write(b)
        return size

    def get_request(self, post_data=''):
        log.debug(['PD', post_data])
        ## parse path..
        p = urlparse.urlparse(self.path)
        q_get = OrderedDict(urlparse.parse_qs(p.query))
        ## flatten..
        for k, v in q_get.items():
            if len(v) == 1:
                q_get[k] = v[0]
            
        ## totally does not currently handle any kind of
        ## multipart posting
        q_post = OrderedDict(urlparse.parse_qs(post_data))
        ## flatten..
        for k, v in q_post.items():
            if len(v) == 1:
                q_post[k] = v[0]

        q_request = OrderedDict(q_get.items() + q_post.items())
        return http.PyNZBRequest(
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

    setproctitle(PROC_TITLE_BASE)

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
