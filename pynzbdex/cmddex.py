from Queue import Empty, Full
from multiprocessing import Process, Queue
from collections import OrderedDict

from pynzbdex import aggregator


LIVE_FOREVER = True
JOB_SIZE = 10000
WORKERS = {}


def spawn_aggregator(q, i, *args, **kwargs):
    ag = aggregator.Aggregator()
    res = ag.scrape_articles(*args, **kwargs)
    q.put(res)
    del ag

def marshall_worker(func, kind, index, group, conf):
    scanid = '%(group)s : %(kind)s(%(index)s)' % {'group': group,
                                                  'kind': kind,
                                                  'index': index}
    if scanid in WORKERS:
        raise RuntimeError('Scanner ID collision: [%s]' % scanid)

    if conf.get('resume', None):
        conf['offset'] = index * JOB_SIZE
        conf['max_processed'] = JOB_SIZE
    q = Queue()
    p = Process(target=spawn_aggregator,
                args=(q, index, group),
                kwargs=conf)
    proc = dict(q=q, p=p,
                args=OrderedDict([
                            ('func', func),
                            ('kind', kind),
                            ('index', index),
                            ('group', group),
                            ('conf', conf)
                        ])
                )
    WORKERS[scanid] = proc
    p.start()
    return (scanid, proc)


if __name__ == '__main__':
    ## GROUP WORK
    #ag = aggregator.Aggregator()
    #ag.scrape_groups()
    #ag.invalidate_groups()
    #del ag


    ## ARTICLE WORK
    ## For now this list is hard coded. But would be based on
    ## each group object's Active flag.
    scanner_config = dict(
        full_scan=dict( 
                        resume=False,
                        get_long_headers=True,
                        invalidate=True,
                        cached=False),
        cache_scan=dict(
                        resume=False,
                        get_long_headers=True,
                        invalidate=True,
                        cached=True),
        resume_scan=dict(
                        resume=True,
                        get_long_headers=True,
                        invalidate=False,
                        cached=True),
        ## SHORT HEADERS ONLY
        invalidate_scan=dict(
                        resume=False,
                        get_long_headers=False,
                        invalidate=True,
                        cached=True),
        quick_scan=dict(
                        resume=True,
                        get_long_headers=False,
                        invalidate=False,
                        cached=True),
        quick_invalidate_scan=dict(
                        resume=True,
                        get_long_headers=False,
                        invalidate=True,
                        cached=True),
        )

    roster = [  ('full_scan', 0),
                ('cache_scan', 0),
                ('resume_scan', 1),
                ('invalidate_scan', 1),
                ('quick_scan', 3), 
                ('quick_invalidate_scan', 1), ]
    for group in ['alt.binaries.teevee', 'alt.binaries.dvd', ]:
        for scanner, num_instances in roster:
            for i in xrange(0, num_instances):
                conf = scanner_config[scanner].copy()
                
                scanid, proc = marshall_worker(spawn_aggregator, scanner,
                                                        i, group, conf)
                print 'Scanner [%s] started.' % scanid

        ## lets only do one group at a time for now,
        ## as my single UNS account only allows for 8 simultaneous conns
        while WORKERS:
            for scanid, proc in WORKERS.items():
                try:
                    res = proc['q'].get(timeout=1)
                except Empty:
                    pass
                else:
                    ## ended normally.  
                    proc['p'].join()
                    del WORKERS[scanid]
                    print 'Scanner [%s] completed.' % scanid, '\n', res

                    ## Keep sayin / Live Forever
                    ## as long as there's still articles to scrape
                    if LIVE_FOREVER and res.get('processed', 0):
                        scanid, proc = marshall_worker(*proc['args'].values())
                        print 'Scanner [%s] started.' % scanid
                    ## the proc died, but it did so cleanly, so continue to next
                    ## iteration -- otherwise will cause false positives for life
                    ## checking
                    continue
                    
                if not proc['p'].is_alive():
                    #killemall...
                    [s['p'].terminate() for n, s in WORKERS.items()]
                    raise RuntimeError('Scanner [%s] died unexpectedly.' % scanid)
