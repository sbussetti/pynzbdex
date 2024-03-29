## Command: Index.
## Inserts NNTP header data into Riak document store
import logging
import sys
import os
import argparse
import time
from Queue import Empty, Full
from multiprocessing import Process, Queue
from collections import OrderedDict

from setproctitle import setproctitle

from pynzbdex.aggregator import (GroupAggregator, ArticleAggregator,
                                 RedisProcessor, ArticleProcessor,
                                 FileProcessor)


LIVE_FOREVER = True
WORKERS = {}
PROC_TITLE_BASE = os.path.basename(__file__)


def spawn(klass, q, i, *args, **kwargs):
    ag = klass(i, *args, **kwargs)
    res = ag.run()
    q.put(res)
    del ag

def marshall_worker(klass, kind, index, group, conf):
    scanid = '%(group)s : %(kind)s(%(index)s)' % {'group': group,
                                                  'kind': kind,
                                                  'index': index}
    if scanid in WORKERS:
        raise RuntimeError('Scanner ID collision: [%s]' % scanid)

    q = Queue()
    p = Process(name='%s - %s' % (PROC_TITLE_BASE, scanid),
                target=spawn,
                args=(klass, q, index, group),
                kwargs=conf)
    proc = dict(q=q, p=p,
                args=OrderedDict([
                            ('klass', klass),
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
    setproctitle(PROC_TITLE_BASE)

    #from pynzbdex import storage
    #from sqlalchemy.schema import CreateTable
    #print CreateTable(storage.sql.File.__table__)


    ## ARTICLE WORK
    ## For now this list is hard coded. But would be based on
    ## each group object's Active flag.
    worker_config = dict(
        full_scan=[ArticleAggregator,
                        dict( 
                        resume=False,
                        get_long_headers=True,
                        invalidate=True,
                        cached=False)],
        cache_scan=[ArticleAggregator,
                        dict(
                        resume=False,
                        get_long_headers=True,
                        invalidate=True,
                        cached=True)],
        resume_scan=[ArticleAggregator,
                        dict(
                        resume=True,
                        get_long_headers=True,
                        invalidate=False,
                        cached=True)],
        ## SHORT HEADERS ONLY
        invalidate_scan=[ArticleAggregator,
                        dict(
                        resume=False,
                        get_long_headers=False,
                        invalidate=True,
                        cached=True)],
        quick_scan=[ArticleAggregator,
                        dict(
                        resume=True,
                        get_long_headers=False,
                        invalidate=False,
                        cached=True)],
        quick_invalidate_scan=[ArticleAggregator,
                        dict(
                        resume=True,
                        get_long_headers=False,
                        invalidate=True,
                        cached=True)],
        quick_full_scan=[ArticleAggregator,
                        dict(
                        resume=False,
                        get_long_headers=False,
                        invalidate=False,
                        cached=False)],
        ## PROCESSORS (Different Signature)
        redis_process=[RedisProcessor,
                        dict()],

        ## TODO: article and file processors should perhaps
        ## be limited to only a single instance, ever.
        article_process=[ArticleProcessor,
                        dict(
                        )],
        file_process=[FileProcessor,
                        dict(
                        )],
        )
    ## options
    aparser = argparse.ArgumentParser(description='commandline nntp indexer')
    aparser.add_argument('groups', metavar='GROUP', type=str, nargs='+',
                   help='groups to work on')
    aparser.add_argument('--loglevel', metavar='LEVEL', type=str,
                   choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                   default='ERROR', help='verbosity level of logging facility')
    aparser.add_argument('--never_retire', action='store_true', 
                   default=False, help='do the same job forever')
    aparser.add_argument('--group_scan', action='store_true', 
                   default=False, help='before doing anything, '
                                       'refresh the list of groups')
    for sc in sorted(worker_config.keys(),
                                key=lambda k: '%s%s' % (k[k.rfind('_'):], k)):
        aparser.add_argument('--%s' % sc, metavar='N', type=int, default=0,
                       help='number of %s workers to spawn' % sc)

    args = aparser.parse_args()

    logging.basicConfig(format=('%(levelname)s:(%(name)s.%(funcName)s'
                                ':%(lineno)d) %(message)s'),
                        level=args.loglevel)

    ## GROUP WORK
    if args.group_scan:
        ag = GroupAggregator(0, invalidate=True, refresh=True)
        ag.run()
        del ag
    
    roster = []
    for group in args.groups:
        for worker_name, worker in worker_config.items():
            work_klass, work_config = worker
            num_instances = getattr(args, worker_name)

            ## doesn't really get used anymore.. perhaps
            ## a final report @ some point? or tui visualization..
            roster.append((worker_name, work_klass, num_instances))

            for i in xrange(0, num_instances):
                conf = work_config.copy()
                workid, proc = marshall_worker(work_klass, worker_name,
                                               i, group, conf)
                print '[%s] started.' % workid

        ##TODO: make marshall_worker not globally access WORKERS list
        ## lets only do one group at a time for now,
        ## as my single UNS account only allows for 8 simultaneous conns
        while WORKERS:
            scanners = [proc for proc in WORKERS.values()
                            if proc['args']['kind'].endswith('_scan')]
            processors = [proc for proc in WORKERS.values()
                            if proc['args']['kind'].endswith('_process')]
            ## we're done here..
            #if not scanners and not processors:
            if not args.never_retire and not scanners:
                [s['p'].terminate() for n, s in WORKERS.items()]
                WORKERS = {}
                print 'Completed work on (%s)' % group

            for workid, proc in WORKERS.items():
                try:
                    res = proc['q'].get(timeout=1)
                except Empty:
                    pass
                else:
                    ## ended normally.  
                    proc['p'].join()
                    del WORKERS[workid]
                    print 'Scanner [%s] returned.' % workid, '::', res

                    ## Keep sayin / Live Forever
                    ## as long as there's still articles to scrape
                    if (LIVE_FOREVER and (not res.get('complete', False)
                                                        or args.never_retire)):
                        workid, proc = marshall_worker(*proc['args'].values())
                        print 'Scanner [%s] re-started.' % workid
                    ## the proc died, but it did so cleanly, so continue to
                    ## next iteration -- otherwise will cause false
                    ## positives for life checking
                    continue
                    
                if not proc['p'].is_alive():
                    #killemall...
                    [s['p'].terminate() for n, s in WORKERS.items()]
                    raise RuntimeError('Scanner [%s] died unexpectedly.' \
                                        % workid)
