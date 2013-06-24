#!/usr/bin/python
"""
Copyright (c) 2013, G Roberts
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.
    * Neither the name of the project nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

__all__ = ['AggregatorDaemon', 'main']

from redis import StrictRedis
from hyperstats.common import unixtime, QueueDaemon, make_facet_id, split_facet, all_permutations
from os import urandom
import marshal, hyperclient, logging

LOG = logging.getLogger(__name__)

class ReliableHyperClient(object):
    """
    'Reliable' version of the HyperDex client that ignores interrupts when
    waiting for a result. This ensures that an operation is always completed
    and the value or return stats is returned.
    """
    def __init__(self, hostname, port):
        self._client = hyperclient.Client(hostname, port)

    def put_if_not_exist(self, space, key, value):
        assert type(space) == str
        assert type(value) == dict
        async = self._client.async_put_if_not_exist(space, key, value)
        while True:
            try:
                return async.wait()
            except hyperclient.HyperClientException, ex:
                if ex.symbol() != 'HYPERCLIENT_INTERRUPTED':
                    raise ex

    def cond_put(self, space, key, condition, value):
        assert type(space) == str
        assert type(condition) == dict
        assert type(value) == dict
        async = self._client.async_cond_put(space, key, condition, value)
        while True:
            try:
                return async.wait()
            except hyperclient.HyperClientException, ex:
                if ex.symbol() != 'HYPERCLIENT_INTERRUPTED':
                    raise ex

    def get(self, space, key):
        assert type(space) == str
        async = self._client.async_get(space, key)
        while True:
            try:
                return async.wait()
            except hyperclient.HyperClientException, ex:
                if ex.symbol() != 'HYPERCLIENT_INTERRUPTED':
                    raise ex


class AggregatorDaemon(QueueDaemon):
    """
    Recieves facets which were put into the queue by the client endpoint.
    It aggregates all the values, buffering them for a period of time, then
    inserts into HyperDex.
    """
    def __init__(self, rdb, hdex):
        super(AggregatorDaemon, self).__init__(rdb)
        assert hdex is not None
        self._hdex = hdex
        self._last_sync = unixtime()

    def aggregate_in_redis(self, record):
        """
        Aggregate the values for all permutations of the records facets.
        """
        with self.redis.pipeline(True) as pipe:
            for facet in all_permutations(record['facets']):   
                facet = split_facet(facet)                       
                self.insert_to_redis(pipe, facet, record['values'])                          
            pipe.execute()
        return True

    def insert_to_hyperdex(self, facet, values):
        """
        Update counters for the facet for the given record.
        """
        values = {key: int(value) for key, value in values.items()}
        put_ok = self._hdex.put_if_not_exist('stats', facet['id'], {
            'facet_parent_id': facet['parent_id'],
            'facet': facet['child'],
            'last_id': make_facet_id([urandom(4), facet['id']]),
            'values': values
        })
        self.incr_stats('hyperdex.ops')
        self.incr_stats('hyperdex.ops.put_if_not_exist')

        # Record already exists, we need to update it
        while put_ok == False:
            server_record = self._hdex.get('stats', facet['id'])
            self.incr_stats('hyperdex.ops')
            self.incr_stats('hyperdex.ops.get')
            if server_record is None:
                # put_if_not_exist failed, but get failed... try again.
                # XXX: avoid infinite loop
                continue            

            new_values = {}
            for value_name, value in values.items():
                new_values[value_name] = int(server_record['values'].get(value_name, 0) + value)

            new_id = make_facet_id([urandom(4), facet['id'], server_record['last_id']])
            put_ok = self._hdex.cond_put('stats', facet['id'],
                               dict(last_id=server_record['last_id']),
                               dict(last_id=new_id,
                                    values=new_values))
            self.incr_stats('hyperdex.ops')
            self.incr_stats('hyperdex.ops.cond_put')
        return True

    def insert_to_redis(self, pipe, facet, values):
        hincr_ops = 0
        for key, value in values:            
            hincr_ops += 1
            pipe.hincrby(facet['id'], key, value)
        pipe.hsetnx(facet['id'], '$hs.facet', marshal.dumps(facet))
        pipe.sadd('keys', facet['id'])

        self.incr_stats('redis.ops.hincrby', hincr_ops)
        self.incr_stats('redis.ops.hsetnx', 2)
        self.incr_stats('redis.ops.sadd')
        self.incr_stats('redis.ops', 3 + hincr_ops)

    def sync_redis(self):
        # Sync every 5 minutes, or when 5k entries exist
        need_to_sync = self.redis.scard('keys') > 5000
        if not need_to_sync:
            if self._last_sync is None:
                need_to_sync = True
            else:
                need_to_sync = self._last_sync < (unixtime() - (60))
        if need_to_sync:
            self.incr_stats('redis.ops')
            self.incr_stats('redis.ops.smembers')
            for member in self.redis.smembers('keys'):
                values = self.redis.hgetall(member)
                self.incr_stats('redis.ops')
                self.incr_stats('redis.ops.hgetall')
                facet = marshal.loads(values['$hs.facet'])
                del values['$hs.facet']
                self.insert_to_hyperdex(facet, values)
                self.show_status()
                if self.is_stopping():
                    break

    def process(self, record):
        result = self.aggregate_in_redis(record)
        self.sync_redis()
        return result

def main(args):
    rdb = StrictRedis()
    hdex = ReliableHyperClient('10.0.3.23', 10502)
    AggregatorDaemon(rdb, hdex).run('aggqueue')

if __name__ == "__main__":
    main()