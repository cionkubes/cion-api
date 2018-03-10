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
    """
    Initializes the database connection. And sets up database, table and
    default data if they do not exist

    :return: connection object
    """
    global conn
    db_host = os.environ['DATABASE_HOST']
    db_port = os.environ['DATABASE_PORT']

    conn = asyncio.get_event_loop().run_until_complete(connection(db_host, db_port))
    asyncio.get_event_loop().run_until_complete(_init_database())


async def ensure_table_exists(table_name, primary_key='id', func=None):
    """
    Creates a table in the database if it does not exist.

    :param table_name: name of the table
    :param primary_key: field to use as primary key, default *id*
    :param func: rethinkdb function to run on the database after the table has
        been created, if it did not previously exist
    :return: the database response
    """
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
    """
    Creates a database by the given name if it does not exist.

    :param db_name: name of the database
    :return: database response
    """
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
    """
    Creates the admin user table row

    :return: table entry representing the admin user
    """
    pw_hash, salt, iterations = auth.create_hash('admin')

    return {
        'username': 'admin',
        'password_hash': pw_hash,
        'salt': salt,
        'iterations': iterations,
        'time_created': r.now().to_epoch_time()
    }


async def _init_database():
    """
    Initializes the cion database and ensures that all tables exist.
    """
    logger.info('Initializing database')

    await ensure_db_exists('cion')
    await ensure_table_exists('tasks')
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
