import _pickle as cPickle

from functools import wraps
from app.dbs import redis_db


def is_cached(key):
    return redis_db('cache').exists(key)


def cache_value(key, value, expire, **kwargs):
    value = cPickle.dumps(value)
    redis_db('cache').set(key, value, expire, **kwargs)


def get_value(key):
    return cPickle.loads(redis_db('cache').get(key))


def cache_deco(key_name, expire=None):

    def _func_wrapp(func):
        @wraps(func)
        async def _wrapper(**kwargs):
            key = key_name.format(**kwargs)
            if is_cached(key):
                return get_value(key)

            result = func(**kwargs)
            cache_value(key, result, expire=expire)
            return result

        return _wrapper

    return _func_wrapp



