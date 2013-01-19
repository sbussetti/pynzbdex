from sqlalchemy.ext.declarative import (declarative_base,
                                        DeferredReflection,
                                        declared_attr)
from sqlalchemy.orm import relationship 
from sqlalchemy import (Column, Integer, String, DateTime,
                        ForeignKey, Table)


class NotFoundError(Exception):
    pass


class MultipleFoundError(Exception):
    pass


def get_or_create(session, model, defaults={}, **kwargs):
    '''get a single object or create if it does not exist'''
    try:
        instance = get(session, model, **kwargs)
    except NotFoundError:
        ckwargs = dict(defaults, **kwargs)
        instance = model(**ckwargs)
        session.add(instance)
    return instance


def get_and_delete(session, model, *args, **kwargs):
    '''get a single object and delete if exists'''
    try:
        instance = get(session, model, *args, **kwargs)
    except NotFoundError:
        pass
    else:
        session.delete(instance)


def get(session, model, *args, **kwargs):
    '''get a single object and complain otherwise'''
    if args:
        q = session.query(model).filter(*args)
    else:
        q = session.query(model).filter_by(**kwargs)

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
    Column('group_id', Integer, ForeignKey('group.id'), nullable=False)
)


class Group(Base):
    name = Column(String(length=255), nullable=False, unique=True)


class File(Base):
    name = Column(String(length=255), nullable=True)
    articles = relationship('Article', backref='file')


class Article(Base):
    mesg_spec = Column(String(length=255), nullable=False, unique=True)
    subject = Column(String(length=511), nullable=False)
    from_ = Column(String(length=255), nullable=False)
    date = Column(DateTime(timezone='UTC'), nullable=False)
    groups = relationship(Group,
                          secondary=group_articles,
                          backref='articles')
    file_id = Column(Integer, ForeignKey('file.id'), nullable=True)

