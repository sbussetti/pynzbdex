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

from jinja2 import Template, Environment, PackageLoader
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import DeferredReflection

from pynzbdex import storage, settings
from pynzbdex.template import templates

class PyNZBDexViewsBase(object):
    methods = ['GET', 'POST']

    def __init__(self, *args, **kwargs):
        super(PyNZBDexViewsBase, self).__init__(*args, **kwargs)

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

    def render(self, ctx={}):
        return self.render_template(self.template_name, ctx)
        

class PyNZBDexHome(PyNZBDexViewsBase):
    template_name = 'index.html'

    def dispatch(self, request, **kwargs):
        return self.render()


class PyNZBDexSearchGroup(PyNZBDexViewsBase):
    template_name = 'search_group.html'

    def get(self, request, *args, **kwargs):
        query = request.GET.get('q', None)
        sort = request.GET.get('s', 'date desc')
        page = int(request.GET.get('p', 1) or 1)
        per_page = int(request.GET.get('pp', 25) or 25)

        pager = PagedResults(query, sort, page, per_page)

        return self.render({'results': pager.all(),
                            'pager': pager,
                            'today': datetime.datetime.today(),
                            'group': group,
                            'request': request,  })


class PyNZBDexSearch(PyNZBDexViewsBase):
    template_name = 'search.html'

    def get(self, request, group_name, doctype, *args, **kwargs):
        query = request.GET.get('q', None)
        sort = request.GET.get('s', 'date desc')
        page = int(request.GET.get('p', 1) or 1)
        per_page = int(request.GET.get('pp', 25) or 25)
        source = request.GET.get('src', 'sql')
        ## doctype one of article, file, report

        group = storage.riak.Group.get(group_name)

        #group = storage.sql.get(self._sql, storage.sql.Group,
        #                        name=group_name)

        if doctype == 'article':
            cq = self._sql.query(storage.sql.Article)\
                        .filter(storage.sql.Article.newsgroups.any(name=group_name))
        elif doctype == 'file':
            cq = self._sql.query(storage.sql.File)\
                        .filter(storage.sql.File.newsgroups.any(name=group_name))
        elif doctype == 'report':
            raise NotImplementedError
        else:
            raise ValueError('Unknown doctype: %s' % doctype)


        pager = PagedResults(cq, sort, page, per_page)

        return self.render({'results': pager.all(),
                            'pager': pager,
                            'today': datetime.datetime.today(),
                            'group': group,
                            'doctype': doctype,
                            'request': request,  })


class PyNZBDexViewArticle(PyNZBDexViewsBase):
    template_name = 'view_article.html'

    def get(self, request, mesg_id, *args, **kwargs):
        delete = request.GET.get('delete', False)

        article = storage.riak.Article.get(mesg_id)

        if delete:
            article.delete()

        return self.render({'request': request,
                            'article': article})


class PyNZBDexViewFile(PyNZBDexViewsBase):
    template_name = 'view_file.html'

    def get(self, request, id, *args, **kwargs):
        delete = request.GET.get('delete', False)

        file_rec = storage.sql.get(self._sql, storage.sql.File, id=id)

        if delete:
            self._sql.delete(file_rec)

        return self.render({'request': request,
                            'file': file_rec})
        

class PagedResults(object):
    def __init__(self, query, sort, page, per_page):
        if per_page > 100:
            per_page = 100
        offset = (page - 1) * per_page

        #q = storage.riak.Article.solrSearch(query, start=offset,
        #                               rows=per_page, sort=sort, wt='xml')
        #total = q.length()
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
        if cw < 0:
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

