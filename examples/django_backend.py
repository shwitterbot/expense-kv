from django.core.cache.backends.base import BaseCache
from django.core.cache.backends.redis import RedisCache


class ExpenseKVCache(RedisCache):
    """Use individual SETs for set_many, without MULTI/EXEC."""

    set_many = BaseCache.set_many
