from redis import StrictRedis
import hyperclient

__all__ = ['redis_connect', 'hyperdex_connect']

def redis_connect():
    return StrictRedis()

def hyperdex_connect():
    return hyperclient.Client('10.0.3.23', 10501)