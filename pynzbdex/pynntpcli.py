import re
import logging
import socket 
import json
import select
import time


logging.basicConfig(format='%(levelname)s: %(message)s')
log = logging.getLogger(__name__)
log.setLevel('INFO')

surrogates = re.compile(r'\\u[a-zA-Z0-9]{5,7}')
def filter_surrogates(unicode_string):
    return surrogates.sub(u'\uFFFD', unicode_string)


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
                    log.exception(e)
                    time.sleep(0.5)
        raise Exception('Could not send message')

    def recv(self):
        data = b''
        while 1:
            ## really thin rety facility
            chunk = b''
            for t in range(0,5):
                try:
                    chunk = self.sock.recv(self._size)
                    if chunk:
                        break
                except socket.error, e:
                    log.exception(e)
                else:
                    time.sleep(0.5)

            if not chunk: 
                break
            else:
                data += chunk
            if data.endswith(b'\x00'):
                break

        if not len(data) or not data.endswith(b'\x00'):
            raise Exception('Connection closed unexpectedly while receiving.')

        data = filter_surrogates(data.decode('utf-8').rstrip('\x00'))
        return data

    def _command(self, cmd, arg=None):
        ## sends a generic command to the server
        ## and receives response.
        ## this has NO retry facility built in, (and it needs it)
        
        
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
                time.sleep(0.5)
                continue

            if len(sdata):
                break

        if not len(sdata):
            raise Exception('server went away')

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

        if RSP == 'OK':
            return seq.get('ARG', None)
        else:
            raise ValueError('nntp proxy sent bad response: %s' % seq['ARG'])

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
