import asyncio
import json

import auth
import rethinkdb as r
from async_rethink import connection
import os
from logzero import logger

r.set_loop_type('asyncio')

conn = None


def init():
    global conn
    db_host = os.environ['DATABASE_HOST']
    db_port = os.environ['DATABASE_PORT']

    conn = asyncio.get_event_loop().run_until_complete(
        connection(db_host, db_port))
    asyncio.get_event_loop().run_until_complete(_init_database())


async def ensure_table_exists(table_name, primary_key='id', func=None,
                              indices=None):
    ret = r.db('cion').table_create(table_name, primary_key=primary_key)
    if func:
        ret = [ret, func]
    await conn.run(
        r.db('cion').table_list().contains(table_name).do(
            lambda table_exists:
            r.branch(
                table_exists,
                {'tables_created': 0},
                ret
            )
        )
    )
    if indices:
        for index in indices:
            ret = r.db('cion').table(table_name).index_create(index)
            await conn.run(
                r.db('cion').table(table_name).index_list().contains(index).do(
                    lambda index_exists:
                    r.branch(
                        index_exists,
                        {'index_created': 0},
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


def create_admin_user_insert():
    pw_hash, salt, iterations = auth.create_hash('admin')

    return {
        "username": 'admin',
        "password_hash": pw_hash,
        "salt": salt,
        "iterations": iterations,
        "time_created": r.now().to_epoch_time()
    }


async def _init_database():
    logger.info('Initializing database')

    await ensure_db_exists('cion')
    await ensure_table_exists('tasks', indices=['time'])
    with open('default_docs.json', 'r') as default:
        await ensure_table_exists('documents', primary_key='name',
                                  func=r.db('cion').table('documents').insert(
                                      json.load(default))
                                  )
    await ensure_table_exists('users', primary_key='username',
                              func=r.db('cion').table('users').insert(
                                  create_admin_user_insert())
                              )
    logger.info('Database initialization complete')
