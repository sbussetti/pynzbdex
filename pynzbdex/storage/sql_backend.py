from sqlalchemy.ext.declarative import (declarative_base, DeferredReflection,
                                        declared_attr)
from sqlalchemy.orm import relationship 
from sqlalchemy import Column, Integer, String, DateTime


Base = declarative_base(cls=DeferredReflection)


class BaseMixin(object):

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    __table_args__ = {'mysql_engine': 'InnoDB'}
    __mapper_args__= {'always_refresh': True}

    id =  Column(Integer, primary_key=True)


class Group(BaseMixin, Base):
    name = Column(String, nullable=False)


class File(BaseMixin, Base):
    name = Column(String, nullable=True)


class Article(BaseMixin, Base):
    mesg_spec = Column(String, nullable=False)
    subject = Column(String, nullable=False)
    from_ = Column(String, nullable=False)
    date = Column(DateTime(timezone='UTC'), nullable=False)
    newsgroups = relationship(Group, secondary='newsgroups')
    file = relationship(File)
