## Command: Index.
## Inserts NNTP header data into Riak document store
import sys
import argparse
from Queue import Empty, Full
from multiprocessing import Process, Queue
from collections import OrderedDict

from pynzbdex import aggregator


LIVE_FOREVER = True
JOB_SIZE = 10000
WORKERS = {}


def spawn_scraper(q, i, *args, **kwargs):
    ag = aggregator.Aggregator()
    res = ag.scrape_articles(*args, **kwargs)
    q.put(res)
    del ag

def spawn_processor(q, i, *args, **kwargs):
    ag = aggregator.Aggregator()
    res = ag.process_redis_cache(*args, **kwargs)
    q.put(res)
    del ag

def spawn_grouper(q, i, *args, **kwargs):
    ag = aggregator.Aggregator()
    res = ag.group_articles(*args, **kwargs)
    q.put(res)
    del ag

def marshall_worker(func, kind, index, group, conf):
    scanid = '%(group)s : %(kind)s(%(index)s)' % {'group': group,
                                                  'kind': kind,
                                                  'index': index}
    if scanid in WORKERS:
        raise RuntimeError('Scanner ID collision: [%s]' % scanid)

    if conf.get('resume', False):
        conf['offset'] = index * JOB_SIZE
        conf['max_processed'] = JOB_SIZE
    q = Queue()
    p = Process(target=func,
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
        full_scan=[spawn_scraper,
                        dict( 
                        resume=False,
                        get_long_headers=True,
                        invalidate=True,
                        cached=False)],
        cache_scan=[spawn_scraper,
                        dict(
                        resume=False,
                        get_long_headers=True,
                        invalidate=True,
                        cached=True)],
        resume_scan=[spawn_scraper,
                        dict(
                        resume=True,
                        get_long_headers=True,
                        invalidate=False,
                        cached=True)],
        ## SHORT HEADERS ONLY
        invalidate_scan=[spawn_scraper,
                        dict(
                        resume=False,
                        get_long_headers=False,
                        invalidate=True,
                        cached=True)],
        quick_scan=[spawn_scraper,
                        dict(
                        resume=True,
                        get_long_headers=False,
                        invalidate=False,
                        cached=True)],
        quick_invalidate_scan=[spawn_scraper,
                        dict(
                        resume=True,
                        get_long_headers=False,
                        invalidate=True,
                        cached=True)],
        ## PROCESSORS (Different Signature)
        redis_processor=[spawn_processor,
                        dict()],
        article_grouper=[spawn_grouper,
                        dict(
                        full_scan=False,
                        complete_only=False
                        )],
        full_article_grouper=[spawn_grouper,
                        dict(
                        full_scan=True,
                        complete_only=False
                        )],
        )
    ## options
    aparser = argparse.ArgumentParser(description='commandline nntp indexer')
    aparser.add_argument('groups', metavar='GROUP', type=str, nargs='+',
                   help='groups to work on')
    for sc in scanner_config.keys():
        aparser.add_argument('--%s' % sc, metavar='N', type=int, default=0,
                       help='number of %s scanners to spawn' % sc)

    args = aparser.parse_args()
    
    roster = []
    for group in args.groups:
        for scanner_name, scanner in scanner_config.items():
            scan_func, scan_config = scanner
            num_instances = getattr(args, scanner_name)

            ## doesn't really get used anymore.. perhaps
            ## a final report @ some point? or tui visualization..
            roster.append((scanner_name, scan_func, num_instances))

            for i in xrange(0, num_instances):
                conf = scan_config.copy()
                scanid, proc = marshall_worker(scan_func, scanner_name,
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
                    if LIVE_FOREVER:# and res.get('processed', 0):
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
