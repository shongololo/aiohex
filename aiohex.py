# coding=utf-8
"""asyncio python hexastore graph implementation using redis's (via aioredis) lexicographical sorted sets"""

__author__ = 'Gareth Simons'

import asyncio
import aioredis
import networkx

class AIOHex:

    def __init__(self, aioredis_conn_pool, key_graph):
        self.aioredis_conn_pool = aioredis_conn_pool
        self.key_graph = key_graph

    async def insert(self, subject, predicate, object):
        """inserts sop triads into redis using all six configurations"""
        # add normalisation of input

        async with self.aioredis_conn_pool.get() as redis_conn:
            await redis_conn.execute('ZADD', self.key_graph,
                # score, values
                0, '|'.join([subject, predicate, object]),
                0, '|'.join([subject, object, predicate]),
                0, '|'.join([predicate, object, subject]),
                0, '|'.join([predicate, subject, object]),
                0, '|'.join([object, predicate, subject]),
                0, '|'.join([object, subject, predicate])
            )

    async def get(self, query_start, query_end):
        async with self.aioredis_conn_pool.get() as redis_conn:
            return await redis_conn.execute('ZRANGEBYLEX', self.key_graph, query_start, query_end)


    async def traverse_breadth(self, search_depth, keys_start, key_relation):
        keys_return = []
        async with self.aioredis_conn_pool.get() as redis_conn:
            if not isinstance(keys_start, list):
                keys_start = list(keys_start)
            keys_set = set(keys_start)
            # convert start keys to tuple and store iteration depth
            keys_start = [(0, key) for key in keys_start]
            while True:
                if not keys_start:
                    break
                breadth, id = keys_start.pop()
                query_start = '[{0}|{1}'.format(id, key_relation)
                query_end = query_start + '|\xff'
                keys_new = await redis_conn.execute('ZRANGEBYLEX', self.key_graph, query_start, query_end)
                # extract new keys
                keys_new = [item.split('|')[2] for item in keys_new]
                # check that keys haven't already been found
                # don't cast new keys to set so that order is retained
                keys_new = [key for key in keys_new if key not in keys_set]
                keys_set.update(keys_new)
                keys_return += keys_new
                keys_start += [(breadth + 1, key) for key in keys_new if breadth < search_depth]
        return keys_return


    # map some graph analysis functions to networkx?

    # mirror to networkx graph?

    # persist networkx to redis?