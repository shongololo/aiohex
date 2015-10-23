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


    async def traverse(self, search_depth, start_id, key_relation):
        async with self.aioredis_conn_pool.get() as redis_conn:
            # container for found links
            visited = set()
            # recursive function def
            async def inner_trace(inner_depth, trigger_links):
                nonlocal key_relation
                if inner_depth == 0:
                    return
                else:
                    inner_depth -= 1
                    for link in trigger_links:
                        query_start = '[{1}|{2}'.format(link, key_relation)
                        query_end = query_start + '|\xff'
                        new_links = await redis_conn.execute('ZRANGEBYLEX', self.key_graph, query_start, query_end)
                        print(inner_depth)
                        # extract keys
                        new_triggers = set([link.split('|')[2] for link in new_links])
                        # filter out links that have already been visited to prevent redundant links
                        filtered_triggers = new_triggers.difference(visited)
                        # update visited set
                        visited.update(filtered_triggers)
                        # send to next recursion
                        await inner_trace(inner_depth, filtered_triggers)
            # call recursive func
            await inner_trace(search_depth, [start_id])
            # return once stack unwinds:
            return visited

    # map some graph analysis functions to networkx?

    # mirror to networkx graph?

    # persist networkx to redis?