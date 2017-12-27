import asyncio
import json

import rethinkdb as r
from rethink_async import connection
import os
from logzero import logger

r.set_loop_type('asyncio')

conn = None


def init():
    global conn
    db_host = os.environ['DATABASE_HOST']
    db_port = os.environ['DATABASE_PORT']

    conn = asyncio.get_event_loop().run_until_complete(connection(db_host, db_port))
    asyncio.get_event_loop().run_until_complete(_init_database())


async def ensure_table_exists(table_name, primary_key='id', func=None):
    ret = r.db('cion').table_create(table_name, primary_key=primary_key)
    if func:
        ret = [ret, func]
    return await conn.run(
        r.db('cion').table_list().contains(table_name).do(
            lambda table_exists: r.branch(
                table_exists,
                {'tables_created': 0},
                ret
            )
        )
    )


async def ensure_db_exists(db_name):
    return await conn.run(
        r.db_list().contains(db_name).do(
            lambda db_exists: r.branch(
                db_exists,
                {'dbs_created': 0},
                r.db_create(db_name)
            )
        )
    )


async def _init_database():
    logger.info('Initializing database')

    await ensure_db_exists('cion')
    await ensure_table_exists('tasks')
    with open('default_docs.json', 'r') as default:
        await ensure_table_exists('documents', primary_key='name',
                                  func=r.db('cion').table('documents').insert(json.load(default)))
    await ensure_table_exists('users', primary_key='username')
    logger.info('Database initialization complete')
