import re
import logging
import socket 
import select
import time
import traceback
from collections import OrderedDict
try:
    import cPickle as pickle
except ImportError:
    import pickle
import io
io.DEFAULT_BUFFER_SIZE = 1024

log = logging.getLogger(__name__)
DELIMITER = b"\x00~~EOM~~\x00"

# we're just stripping all untranslated unicode @ this point
# and we don't know what the width is, either..
surrogates = re.compile(r'\\u[a-zA-Z0-9]{4,8}?', re.I)
def filter_surrogates(unicode_string):
    return surrogates.sub(u'\u0244', unicode_string)
    #return unicode_string


class NNTPProxyClient(object):
    def __init__(self, host='localhost', port=1701, pickle_protocol=2):
        self._pickle_protocol = pickle_protocol
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
        data = data + DELIMITER #.encode('utf-8')
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
        #data = bytearray()
        data = io.BytesIO()
        eom = -1
        while 1:
            ## really thin rety facility
            #chunk = bytearray(self._size)
            bytes_recv = 0
            for t in range(0,5):
                try:
                    
                    #self.sock.recv_into(chunk, self._size)
                    #bytes_recv = self.sock.recv_into(data, self._size)
                    bytes_recv = data.write(self.sock.recv(self._size))
                    if bytes_recv:
                        break
                    #if chunk:
                    #    break
                except socket.error, e:
                    log.debug(traceback.format_exc())
                else:
                    time.sleep(0.5)
            #if not chunk: 
            #    break
            #else:
            #    data.extend(chunk)
            if not bytes_recv:
                break

            #data = data.getvalue()
            #log.debug(['DATA', data])
            try:
                eom = data.getvalue().index(DELIMITER)
            except ValueError, IndexError:
                pass
            else:
                break

        if not eom >= 0:
            raise Exception('Connection closed unexpectedly while receiving.')

        #data = filter_surrogates(data.rstrip(b'\x00').decode('utf8'))
        data.seek(0)
        return data #.decode('utf8') #.rstrip(b'\x00')

    def _command(self, cmd, arg=None):
        ## sends a generic command to the server
        ## and receives response.
        
        ## SEND

        ## TODO: should output also be sent via bytestring? think yes.
        req = {u'CMD': cmd}
        if arg is not None:
            req[u'ARG'] = arg
        try:
            rdata = pickle.dumps(req, protocol=self._pickle_protocol)
        except pickle.PicklingError:
            log.error(req)
            raise

        sdata = io.BytesIO()
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

            try:
                seq = pickle.load(sdata)
            except pickle.UnpicklingError:
                log.error(sdata)
                raise
            except EOFError:
                continue

            try:
                RSP = seq['RSP']
            except KeyError:
                log.error(seq)
                raise

            if RSP in ['OK', 'ERR']:
                resp = seq.get('ARG', None)
                try:
                    done = resp['code'] == '420'
                except (TypeError, KeyError):
                    done = False

                if not done:
                    return resp
                else:
                    return []
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

