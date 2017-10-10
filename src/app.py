import binascii
import hashlib
import json

import rethinkdb as r
import socketio
from aiohttp import web
from logzero import logger
from numpy import random

conn = None

sio = socketio.AsyncServer(async_mode='aiohttp')
app = web.Application()
sio.attach(app)

sessions = {}


def create_session(user):
    token = binascii.hexlify(os.urandom(64)).decode()
    sessions[token] = user
    return token


def retrieve_session(token):
    return sessions[token]


def create_hash(to_hash: str):
    iterations = random.randint(low=20000, high=25000)
    salt = os.urandom(32)
    hash_created = hash_str(to_hash, salt, iterations)
    return hash_created, salt, iterations


def hash_str(to_hash: str, salt, iterations):
    return hashlib.pbkdf2_hmac('sha512', to_hash.encode(), salt, iterations, 128)


async def new_task_watch():
    global conn
    logger.info('starting task watch loop')

    db_host = 'localhost'
    db_port = '28016'

    r.set_loop_type('asyncio')

    conn = await r.connect(db_host, db_port)
    cursor = await r.db('cion').table('tasks').changes().run(conn)

    while await cursor.fetch_next():
        change = await cursor.next()

        logger.debug(f"Change in tasks table: {change}")
        row = change['new_val']

        logger.debug(f"Dispatching row: {row}")
        await sio.emit(event='task_update', data=row, broadcast=True)
        logger.debug('Row delivered')


resp_bad_creds = web.Response(status=401,
                              text=json.dumps({'reason': 'Bad credentials.'}),
                              content_type='application/json')


async def api_auth(request):
    bod = await request.json()
    username = bod['username']
    password = bod['password']

    user = await r.db('cion').table('users').get(username).run(conn)

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

    db_res = await r.db('cion').table('users').insert({
        "username": username,
        "password_hash": pw_hash,
        "salt": salt,
        "iterations": iterations
    }).run(conn)

    print(db_res)

    return web.Response(status=201)


async def get_tasks(request):
    print('status requested')
    task_cursor = await r.db('cion').table('tasks', read_mode='majority').run(conn)

    t = []
    while await task_cursor.fetch_next():
        t.append(await task_cursor.next())

    return web.Response(status=200,
                        text=json.dumps(t),
                        content_type='application/json')


@sio.on('connect')
def user_connected(sid, environ):
    print('Client connected')


@sio.on('disconnect')
def disconnect(sid):
    print('Client disconnect')


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

    app.router.add_post('/api/v1/auth', api_auth)
    app.router.add_post('/api/v1/usercreate', api_create_user)
    app.router.add_get('/api/v1/tasks', get_tasks)

    sio.start_background_task(new_task_watch)
    web.run_app(app, host='0.0.0.0', port=5000)
