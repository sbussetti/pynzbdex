## proxy tests and whatnot
import logging

from pynzbdex.pynntpcli import NNTPProxyClient
from pynzbdex import settings, aggregator

logging.basicConfig(format=('%(levelname)s:(%(name)s.%(funcName)s) '
                            '%(message)s'), level='DEBUG')

nntp_cfg = settings.NNTP_PROXY['default']
print nntp_cfg
nntp = NNTPProxyClient(host=nntp_cfg['HOST'],
                       port=nntp_cfg['PORT'])


print nntp.group('alt.binaries.teevee')

ag = aggregator.Aggregator()
ag._sql

