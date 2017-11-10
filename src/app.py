import binascii
import hashlib
import asyncio
import json
import random

import rethinkdb as r
from aiohttp import web
from logzero import logger

import websocket
from rethink_async import connection

app = web.Application()
r.set_loop_type('asyncio')

conn = None

sessions = {}


def create_session(user):
    token = binascii.hexlify(os.urandom(64)).decode()
    sessions[token] = user
    return token


def retrieve_session(token):
    return sessions[token]


def create_hash(to_hash: str):
    iterations = random.randint(20000, 25000)
    salt = os.urandom(32)
    hash_created = hash_str(to_hash, salt, iterations)
    return hash_created, salt, iterations


def hash_str(to_hash: str, salt, iterations):
    return hashlib.pbkdf2_hmac('sha512', to_hash.encode(), salt, iterations, 128)


resp_bad_creds = web.Response(status=401,
                              text=json.dumps({'reason': 'Bad credentials.'}),
                              content_type='application/json')


async def api_auth(request):
    bod = await request.json()
    username = bod['username']
    password = bod['password']

    user = await conn.run(conn.db().table('users').get(username))

    if not user:
        return resp_bad_creds

    salt = user['salt']
    iterations = user['iterations']
    stored_hash = user['password_hash']

    input_hash = hash_str(password, salt, iterations)

    if not input_hash == stored_hash:
        return resp_bad_creds

    token = create_session(user)
    return web.Response(status=200,
                        text=json.dumps({'token': token}),
                        content_type='application/json')


async def api_create_user(request):
    bod = await request.json()
    username = bod['username']
    pw_hash, salt, iterations = create_hash(bod['password'])

    db_res = await conn.run(conn.db().table('users').insert({
        "username": username,
        "password_hash": pw_hash,
        "salt": salt,
        "iterations": iterations
    }))

    print(db_res)

    return web.Response(status=201)


async def get_tasks(request):
    print('status requested')

    t = []
    async for task in conn.run_iter(conn.db().table('tasks', read_mode='majority')):
        t.append(task)

    return web.Response(status=200,
                        text=json.dumps(t),
                        content_type='application/json')


if __name__ == '__main__':
    import sys
    import os

    prod = len(sys.argv) > 1 and sys.argv[1].lower() == 'prod'

    if not prod:
        static_path = os.path.join('..', '..', 'frontend', 'src', 'www')

        with open(os.path.join(static_path, 'spa-entry.html')) as f:
            indexfile = f.read()

        async def index(request):
            return web.Response(text=indexfile, content_type='text/html')

        app.router.add_get('/', index)
        app.router.add_static('/resources', os.path.join(static_path, 'resources'))

    db_host = os.environ['DATABASE_HOST']
    db_port = os.environ['DATABASE_PORT']

    conn = asyncio.get_event_loop().run_until_complete(connection(db_host, db_port))

    wsroute = websocket.create(conn)
    app.router.add_get('/api/v1/socket', wsroute)

    app.router.add_post('/api/v1/auth', api_auth)
    app.router.add_post('/api/v1/usercreate', api_create_user)
    app.router.add_get('/api/v1/tasks', get_tasks)

    web.run_app(app, host='0.0.0.0', port=5000)
