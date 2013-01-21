import re
import logging
import socket 
import json
import select
import time
import traceback


log = logging.getLogger(__name__)

# we're just stripping all untranslated unicode @ this point
# and we don't know what the width is, either..
surrogates = re.compile(r'\\u[a-zA-Z0-9]{4,8}?', re.I)
def filter_surrogates(unicode_string):
    return surrogates.sub(u'\u0244', unicode_string)
    #return unicode_string


class NNTPProxyClient(object):
    def __init__(self, host='localhost', port=1701):
        self._host = host
        self._port = port
        self._size = 1024
        self._s = None

    def __del__(self):
        ## clean up if at all possible..
        self.close()

    def _connect(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        s.connect((self._host,self._port)) 
        return s

    @property
    def sock(self):
        ## retries and all will go here...
        if self._s == None:
            log.debug('reconnect')
            self._s = self._connect()
        ## we block on our end even if they don't
        return self._s

    def close(self):
        if self._s:
            self._s.close()
            self._s = None

    def send(self, data):
        ## this is taking advantage of that fact that all
        ## requests are smaller than frame size..
        data = (u'%s\x00' % data).encode('utf-8')
        if len(data) > self._size:
            raise Exception('Request too large to send')
        ## try and then reconnect if needed..
        for t in range(0,5):
            try:
                r = self.sock.send(data) 
                return r
            except socket.error, e:
                if e.errno == 32:
                    self.close()
                    break
                else:
                    log.debug(traceback.format_exc())
                    time.sleep(0.5)
        raise Exception('Could not send message')

    def recv(self):
        data = bytearray()
        while 1:
            ## really thin rety facility
            chunk = bytearray(self._size)
            for t in range(0,5):
                try:
                    #chunk = self.sock.recv(self._size)
                    self.sock.recv_into(chunk, self._size)
                    if chunk:
                        break
                except socket.error, e:
                    log.debug(traceback.format_exc())
                else:
                    time.sleep(0.5)

            if not chunk: 
                break
            else:
                data.extend(chunk)
            if data.endswith(b'\x00'):
                break

        if not len(data) or not data.endswith(b'\x00'):
            raise Exception('Connection closed unexpectedly while receiving.')

        data = filter_surrogates(data.rstrip(b'\x00').decode('utf8'))
        return data

    def _command(self, cmd, arg=None):
        ## sends a generic command to the server
        ## and receives response.
        
        ## SEND
        req = {u'CMD': cmd}
        if arg is not None:
            req[u'ARG'] = arg
        try:
            rdata = json.dumps(req, ensure_ascii=False)
        except ValueError:
            log.error(req)
            raise

        sdata = ''
        for i in range(0, 5):
            try:
                log.debug('SEND')
                self.send(rdata) 
                log.debug('RECV')
                sdata = self.recv()
            except:
                log.debug(traceback.format_exc())
                time.sleep(0.5)
                continue

            if len(sdata):
                try:
                    seq = json.loads(sdata)
                except ValueError:
                    log.error(sdata)
                    raise
                try:
                    RSP = seq['RSP']
                except KeyError:
                    log.error(seq)
                    raise

                if RSP in ['OK', 'ERR']:
                    return seq.get('ARG', None)
                else:
                    log.error('nntp proxy sent bad response: %s' % seq['ARG'])
                    ## retry the entire command..
                    time.sleep(0.5)
                    continue

        ## if we've reached this point we failed
        raise Exception('server went away')

    def get_groups(self, prefix=None):
        d = {'prefix': prefix}
        resp = self._command('GETGROUPS', d)
        return resp

    def group(self, group_name):
        d = {'group_name': group_name}
        resp = self._command('GROUP', d)
        return resp

    def get_group(self, msg, group_name=None):
        d = {'message_spec': msg, 'group_name': group_name}
        resp = self._command('GETGROUP', d)
        return resp

    def get_header(self, msg, group_name=None):
        d = {'message_spec': msg, 'group_name': group_name}
        resp = self._command('GETHEADER', d)
        return resp

