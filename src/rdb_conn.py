import asyncio
import rethinkdb as r
from rethink_async import connection
import os

r.set_loop_type('asyncio')

conn = None


def init():
    global conn
    db_host = os.environ['DATABASE_HOST']
    db_port = os.environ['DATABASE_PORT']

    conn = asyncio.get_event_loop().run_until_complete(connection(db_host, db_port))
