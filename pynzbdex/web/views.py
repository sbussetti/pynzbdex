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
import logging
import itertools

from jinja2 import Template, Environment, PackageLoader
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import DeferredReflection
from sqlalchemy.sql.expression import func

from pynzbdex import storage, settings
from pynzbdex.web import http
from pynzbdex.web.template import templates


log = logging.getLogger(__name__)

class ViewsBase(object):
    methods = ['GET', 'POST']

    def __init__(self, *args, **kwargs):
        super(ViewsBase, self).__init__(*args, **kwargs)

    def storage_setup(self):
        sql_cfg = settings.DATABASE['default']
        dsn = '%(DIALECT)s+%(DRIVER)s://%(USER)s:%(PASS)s@%(HOST)s:%(PORT)s/%(NAME)s' % sql_cfg
        sql_engine = create_engine(dsn)
        
        ## stop running syncdb at startup after development.
        storage.sql.Base.metadata.create_all(sql_engine)
 
        DeferredReflection.prepare(sql_engine)
        Session = sessionmaker(bind=sql_engine)
        self._sql = Session()

    def storage_teardown(self):
        if self._sql:
            self._sql.commit()
            self._sql.close() 

    def dispatch(self, request, *args, **kwargs):
        self.storage_setup()

        try:
            if request.method in self.methods: 
                func = getattr(self, request.method.lower(), None)
                if func:
                    return func(request, *args, **kwargs)
                else:
                    raise Exception('Unhandled method: %s' % request.method)
            else:
                raise Exception('Unknown method: %s' % request.method)
        except:
            raise
        finally:
            self.storage_teardown()

    def render_template(self, name, ctx={}):
        tmpl = templates.get_template(name)
        return tmpl.render(**ctx)

    def render(self, ctx, request, status=200, content_type='text/html', headers={}):
        if self.template_name:
            ## CONTEXT PROCESSORS
            ctx.update({'request': request}) 

            body = self.render_template(self.template_name, ctx)
        else:
            body = unicode(ctx)
        return http.Response(body=body, status_code=status,
                                  content_type=content_type, headers=headers)
        

class Home(ViewsBase):
    template_name = 'index.html'

    def get(self, request, **kwargs):
        #woah does this need to be cached
        groups = self._sql.query(storage.sql.Group, 'name', func.count(storage.sql.group_reports.c.report_id).label('total')).outerjoin(storage.sql.group_reports).group_by(storage.sql.Group).order_by('name ASC')

        return self.render({'groups': groups}, request)


class SearchGroups(ViewsBase):
    template_name = 'search_groups.html'

    def get(self, request, *args, **kwargs):
        query = request.GET.get('q', None)
        sort = request.GET.get('s', 'date desc')
        page = int(request.GET.get('p', 1) or 1)
        per_page = int(request.GET.get('pp', 25) or 25)

        raise NotImplementedError

        pager = PagedResults(query, sort, page, per_page)

        return self.render({'results': pager.all(),
                            'pager': pager,
                            'today': datetime.datetime.today(),
                            'group': group,
                            }, request)


class SearchRoute(ViewsBase):
    def post(self, *args, **kwargs):
        return self.get(*args, **kwargs)

    def get(self, request, *args, **kwargs):
        query = request.REQUEST.get('q', None)
        doctype = request.REQUEST.get('dt', None)
        group = request.REQUEST.get('g', None)

        if not doctype or not query:
            return http.Redirect(request.headers['referer'])
        else:
            ##TODO: sloppy
            from pynzbdex.web.router import router
            if group:
                url_base = router.url_reverse('search_group', doctype, group)
            else:
                url_base = router.url_reverse('search', doctype)
            return http.Redirect('%s?q=%s' % (url_base, query))


class Search(ViewsBase):
    template_name = 'search.html'
    '''
        NZB format as listed by sabnzbd
        (DTD mirror: http://www.usenetshack.com/media/docs/DTD/nzb/nzb-1.1.dtd)

        <?xml version="1.0" encoding="iso-8859-1" ?>
        <!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">
        <nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
         <head>
           <meta type="title">Your File!</meta>
           <meta type="tag">Example</meta>
         </head>
         <file poster="Joe Bloggs &amp;lt;bloggs@nowhere.example&amp;gt;" date="1071674882" subject="Here's your file!  abc-mr2a.r01 (1/2)">
           <groups>
             <group>alt.binaries.newzbin</group>
             <group>alt.binaries.mojo</group>
           </groups>
           <segments>
             <segment bytes="102394" number="1">123456789abcdef@news.newzbin.com</segment>
             <segment bytes="4501" number="2">987654321fedbca@news.newzbin.com</segment>
           </segments>
         </file>
        </nzb>
    '''

    def post(self, request, doctype, group_name=None, *args, **kwargs):
        ids = request.POST.get('f', [])
        action = request.POST.get('a', '')

        if not ids:
            raise ValueError('At least one id required')

        #TODO: dum
        if action == 'nzb':
            if not isinstance(ids, list):
                ids = [ids, ]

            ## expects a list of IDs for report and file records
            ## files, gather lists of associated articles.
            files = []
            title = 'selected_files_%s' % int(time.time())
            if doctype == 'report':
                files = self._sql.query(storage.sql.File)\
                            .filter_by(report_id=ids[0])\
                            .order_by(storage.sql.File.subject).all()
            elif doctype == 'file':
                files = self._sql.query(storage.sql.File)\
                                .filter(storage.sql.File.id.in_(ids))\
                                .order_by(storage.sql.File.subject).all()
            else:
                raise ValueError('Unknown doctype: %s' % doctype)

            ##TODO: factor nzb builder out into its own class
            from lxml import etree

            tree = etree.XML('''\
            <nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
            </nzb>
            ''')

            head = etree.SubElement(tree, 'head')
            etree.SubElement(head, 'meta', {'type': 'title'}).text = title

            for file_rec in files:
                file_ele = etree.SubElement(tree, 'file',
                                {'poster': file_rec.from_, 
                                 'date': u'%d' % time.mktime(file_rec.date.timetuple()), 
                                 'subject': file_rec.subject})
                groups = etree.SubElement(file_ele, 'groups')
                for group in file_rec.newsgroups.order_by(storage.sql.Group.name).all():
                    etree.SubElement(groups, 'group').text = group.name

                segments = etree.SubElement(file_ele, 'segments')
                for segment in file_rec.articles.order_by(storage.sql.Article.part).all():
                    log.debug([segment.bytes_, segment.part, segment.mesg_spec])
                    etree.SubElement(segments, 'segment',
                                {'bytes': u'%d' % segment.bytes_,
                                 'number': u'%d' % segment.part}).text = segment.mesg_spec.lstrip('<').rstrip('>')

            xml_string = etree.tostring(tree, encoding="utf-8",
                                 xml_declaration=True,
                                 pretty_print=True,
                                 doctype='<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">')    
            
            return self.render(xml_string, request, content_type='application/xml',
                               headers={'Content-Disposition': 'attachment; filename="%s.nzb"' % title})
        elif action == 'del':
            if doctype == 'article':
                ## don't forget riak
                raise NotImplementedError
            elif doctype == 'file':
                deleted = self._sql.query(storage.sql.File)\
                                    .filter(storage.sql.File.id.in_(ids))\
                                    .delete(synchronize_session=False)

            elif doctype == 'report':
                deleted = self._sql.query(storage.sql.Report)\
                                    .filter(storage.sql.Report.id.in_(ids))\
                                    .delete(synchronize_session=False)
            else:
                raise ValueError('Unknown doctype: %s' % doctype)
        else:
            raise ValueError('Unknown action: %s' % action)

        ##TODO: sessions, messaging, idk..
        return self.get(request, doctype, group_name, *args, **kwargs)

    def get(self, request, doctype, group_name=None, *args, **kwargs):
        query = request.GET.get('q', '')
        sort = request.GET.get('s', 'date desc')
        page = int(request.GET.get('p', 1) or 1)
        per_page = int(request.GET.get('pp', 25) or 25)
        source = request.GET.get('src', 'sql')
        ## doctype one of article, file, report

        group = None
        stats = {}

        if doctype == 'article':
            cq = self._sql.query(storage.sql.Article)
            if group_name:
                group = storage.riak.Group.get(group_name)
                cq = cq.filter(storage.sql.Article.newsgroups.any(name=group_name))
                ## coverage stats
                ## this is not accurate per- se as last_stored resets on
                ## every pass ... need to keep better count
                indexed = group.last_stored - group.first
                remaining = group.last - group.last_stored
                total = group.last - group.first
                stats = dict(
                        indexed_pct=int(round((indexed / float(total)) * 100)),
                        remaining_pct=int(round((remaining / float(total)) * 100)),
                        indexed=indexed,
                        remaining=remaining,
                        total=total,
                    )

            if query:
                cq = cq.filter(storage.sql.Article.subject.like('%%%s%%' % query))
        elif doctype == 'file':
            cq = self._sql.query(storage.sql.File)
            if group_name:
                group = storage.sql.get(self._sql, storage.sql.Group,
                                        name=group_name)
                cq = cq.filter(storage.sql.File.newsgroups.any(name=group_name))
            ## TODO file stats will be just the percentage of articles with
            ## and without association to a file
            stats = {}
            if query:
                cq = cq.filter(storage.sql.File.subject.like('%%%s%%' % query))
        elif doctype == 'report':
            cq = self._sql.query(storage.sql.Report)
            if group_name:
                group = storage.sql.get(self._sql, storage.sql.Group,
                                        name=group_name)
                cq = cq.filter(storage.sql.Report.newsgroups.any(name=group_name))
            ## TODO stats
            stats = {}

            if query:
                cq = cq.filter(storage.sql.Report.subject.like('%%%s%%' % query))
        else:
            raise ValueError('Unknown doctype: %s' % doctype)



        pager = PagedResults(cq, sort, page, per_page)


        return self.render({'results': pager.all(),
                            'pager': pager,
                            'today': datetime.datetime.today(),
                            'group': group,
                            'stats': stats,
                            'doctype': doctype,
                            'query': query},
                           request)


class ViewArticle(ViewsBase):
    template_name = 'view_article.html'

    def get(self, request, mesg_id, *args, **kwargs):
        delete = request.GET.get('delete', False)

        article = storage.riak.Article.get(mesg_id)

        if delete:
            article.delete()

        return self.render({'article': article}, request)


class ViewFile(ViewsBase):
    template_name = 'view_file.html'

    def get(self, request, id, *args, **kwargs):
        delete = request.GET.get('delete', False)

        file_rec = storage.sql.get(self._sql, storage.sql.File, id=id)

        if delete:
            self._sql.delete(file_rec)

        return self.render({'file': file_rec}, request)


class ViewReport(ViewsBase):
    template_name = 'view_report.html'

    def get(self, request, id, *args, **kwargs):
        delete = request.GET.get('delete', False)

        report = storage.sql.get(self._sql, storage.sql.Report, id=id)

        if delete:
            self._sql.delete(report)

        return self.render({'report': report}, request)


class PagedResults(object):
    def __init__(self, query, sort, page, per_page):
        if per_page > 100:
            per_page = 100
        offset = (page - 1) * per_page

        q = query.order_by()
        total = q.count()

        prev_offset = offset - per_page
        next_offset = offset + per_page
        current_offset = offset + 1
        
        last_page = int(math.ceil(total / per_page)) + 1
        current_page = int(math.ceil(offset / per_page)) + 1
        pages = []

        if prev_offset < 0:
            prev_offset = None
            prev_page = None
        else:
            prev_page = int(math.ceil(prev_offset / per_page)) + 1

        if next_offset > total:
            next_offset = None
            next_page = None
        else:
            next_page = int(math.ceil(next_offset / per_page)) + 1

        rem = (total % per_page)
        if rem:
            last_offset = total - rem
        else:
            last_offset = total - per_page

        ew = current_page + 5
        cw = current_page - 3
        if cw <= 0:
            ew += -1 * cw
            cw = 1
        if ew > last_page:
            ew = last_page
        for i in range(cw, ew+1):
            pages.append((i, i * per_page))

        self.q = q
        self.per_page = per_page
        self.offset = offset
        self.total = total
        self.prev_offset = prev_offset
        self.next_offset = next_offset
        self.prev_page = prev_page
        self.next_page = next_page
        self.current_offset = current_offset
        self.last_page = last_page
        self.current_page = current_page
        self.pages = pages

    def all(self):
        #return self.q.all()
        #for aref in self.q[self.offset:(self.offset + self.per_page)]:
        #    yield storage.riak.Article.get(aref.mesg_spec)
        return self.q[self.offset:(self.offset + self.per_page)]

