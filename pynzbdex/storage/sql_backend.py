import logging
from datetime import datetime

from sqlalchemy.ext.declarative import (declarative_base,
                                        DeferredReflection,
                                        declared_attr, synonym_for)
from sqlalchemy.orm import relationship 
from sqlalchemy import (Column, Integer, BigInteger, Unicode, DateTime, String,
                        ForeignKey, Table, Boolean, UniqueConstraint, Index)
from sqlalchemy.sql import exists as sqla_exists


log = logging.getLogger(__name__)


class NotFoundError(Exception):
    pass


class MultipleFoundError(Exception):
    pass


def exists(session, where):
    return session.query(sqla_exists().where(where)).scalar()


def get_or_create(session, model, defaults={}, **kwargs):
    '''get a single object or create if it does not exist'''
    try:
        instance = get(session, model, **kwargs)
        created = False
    except NotFoundError:
        ckwargs = dict(defaults, **kwargs)
        instance = model(**ckwargs)
        session.add(instance)
        created = True
    return instance, created

def get_and_delete(session, model, *args, **kwargs):
    '''get a single object and delete if exists'''
    try:
        instance = get(session, model, *args, **kwargs)
    except NotFoundError:
        pass
    else:
        session.delete(instance)
        return instance


def get(session, model, *args, **kwargs):
    '''get a single object and complain otherwise'''
    q = session.query(model).filter(*args)
    if kwargs:
        q = q.filter_by(**kwargs)

    if q.count() < 1:
        raise NotFoundError
    elif q.count() > 1:
        raise MultipleFoundError
    else:
        return q.first()


class BaseMixin(DeferredReflection):

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    __table_args__ = {'mysql_engine': 'InnoDB'}

    id =  Column(Integer, primary_key=True)


Base = declarative_base(cls=BaseMixin)


group_articles = Table('group_articles', Base.metadata,
    Column('article_id', Integer, ForeignKey('article.id'), nullable=False),
    Column('group_id', Integer, ForeignKey('group.id'), nullable=False),
    UniqueConstraint('article_id', 'group_id'),
)

group_files = Table('group_files', Base.metadata,
    Column('group_id', Integer, ForeignKey('group.id'), nullable=False),
    Column('file_id', Integer, ForeignKey('file.id'), nullable=False),
    UniqueConstraint('file_id', 'group_id'),
)

group_reports = Table('group_reports', Base.metadata,
    Column('group_id', Integer, ForeignKey('group.id'), nullable=False),
    Column('report_id', Integer, ForeignKey('report.id'), nullable=False),
    UniqueConstraint('report_id', 'group_id'),
)

class Group(Base):
    name = Column(Unicode(length=255),
                    nullable=False, unique=True)

    def __repr__(self):
        return u'%s' % self.name


class Article(Base):
    mesg_spec = Column(Unicode(length=255), nullable=False, unique=True)
    subject = Column(Unicode(length=511), nullable=False)
    from_ = Column(Unicode(length=127), nullable=False)
    bytes_ = Column(BigInteger, nullable=False, default=0)
    date = Column(DateTime(timezone='UTC'), nullable=False)
    part = Column(Integer, nullable=True) ## part X of..
    newsgroups = relationship(Group,
                          secondary=group_articles,
                          lazy='dynamic',
                          backref='articles')
    file_id = Column(Integer,
                    ForeignKey('file.id', ondelete='SET NULL'),
                    nullable=True)

    def __repr__(self):
        return u'%s' % self.subject

    @synonym_for('mesg_spec')
    @property
    def key(self):
        return self.mesg_spec


class File(Base):
    __table_args__ = ()

    subject = Column(Unicode(length=511), nullable=False)
    from_ = Column(Unicode(length=127), nullable=False)

    subj_key = Column(String(length=767, collation='latin1_swedish_ci'),
                      nullable=True, unique=True)

    complete = Column(Boolean, default=False, nullable=False)
    parts = Column(Integer, nullable=True) ## ... of X parts
    articles = relationship('Article', 
                          lazy='dynamic',
                          backref='file')
    newsgroups = relationship(Group,
                          secondary=group_files,
                          lazy='dynamic',
                          backref='files')
    bytes_ = Column(BigInteger, nullable=False, default=0)
    date = Column(DateTime(timezone='UTC'), nullable=False)
    report_id = Column(Integer,
                    ForeignKey('report.id', ondelete='SET NULL'),
                    nullable=True)

    def __repr__(self):
        return u'%s' % self.subject

    @synonym_for('id')
    @property
    def key(self):
        return self.id


class Report(Base):
    __table_args__ = (
            UniqueConstraint('subject'),
        )

    subject = Column(Unicode(length=255), nullable=False)
    complete = Column(Boolean, default=False, nullable=False)
    files = relationship('File', 
                          lazy='dynamic',
                          backref='report')
    newsgroups = relationship(Group,
                          secondary=group_reports,
                          lazy='dynamic',
                          backref='reports')
    bytes_ = Column(BigInteger, nullable=False, default=0)
    date = Column(DateTime(timezone='UTC'), nullable=False, default=datetime.today())

    def __repr__(self):
        return u'%s' % self.subject

    @synonym_for('mesg_spec')
    @property
    def key(self):
        return self.id

