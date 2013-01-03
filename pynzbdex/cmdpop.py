## Command: Population.
## Reads Redis cache for commands and updates the RDBMS accordingly
import json

from redis import StrictRedis
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from pynzbdex import settings, storage


redis_indexbuckets = 1000 ## TODO: should be in settings 


def get_or_create(session, model, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance
    else:
        instance = model(**kwargs)
        return instance


if __name__ == '__main__':

    engine = create_engine(settings.DATABASE['default'])
    storage.sql.Base.metadata.create_all(engine)
    _db = sessionmaker(bind=engine)

    redis_cfg = settings.REDIS['default']
    _redis = StrictRedis(host=redis_cfg['HOST'],
                         port=redis_cfg['PORT'],
                         db=redis_cfg['DB'])

    for b in xrange(0, redis_indexbuckets):
        for mesg_spec in _redis.smembers('newart:%s' % b):
            article = _redis.get(mesg_spec)
            if article.startswith('EXPIRE'):
                
            else:
                article.update({'mesg_spec': mesg_spec})

                group_names = article.pop('newsgroups')
                for group_name in group_names:
                    group = get_or_create(_db, storage.sql.Group, name=
