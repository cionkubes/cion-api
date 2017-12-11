import asyncio
import binascii
import hashlib
import json
import random
import re

import rethinkdb as r
from aiohttp import web
from rethink_async import connection
from rethinkpool import RethinkPool

import websocket

pool = RethinkPool()

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


async def image_and_tag(full_image):
    glob = await get_doc_from_db('glob')
    r = re.match(glob, full_image)
    return r.group(0), r.group(1)


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


async def get_doc_from_db(doc_name):
    return await conn.run(conn.db().table('documents').get(doc_name))


async def get_document(request):
    doc = await get_doc_from_db(request.match_info['name'])
    return web.Response(status=200,
                        text=json.dumps(doc),
                        content_type='application/json')


async def set_document(request):
    bod = await request.json()
    db_res = await conn.run(conn.db().table('documents').get(bod['name']).replace(bod))
    print(db_res)
    return web.Response(status=201)


async def get_tasks(request):
    t = []
    async for task in conn.run_iter(conn.db().table('tasks', read_mode='majority')):
        t.append(task)

    return web.Response(status=200,
                        text=json.dumps(t),
                        content_type='application/json')


async def get_documents(request):
    t = []
    async for document in conn.run_iter(conn.db().table('documents', read_mode='majority')):
        t.append(document)
    return web.Response(status=200,
                        text=json.dumps(t),
                        content_type='application/json')


async def get_service_conf(service_name):
    db_res = await conn.run(conn.db().table('documents').get('images')['document'])
    if service_name in db_res:
        return db_res[service_name]
    else:
        return {}


async def get_db_running_image(service_name, service_conf=None):
    if not service_conf:
        service_conf = await get_service_conf(service_name)
    if not service_conf:
        return ''
    glob = await get_doc_from_db('glob')
    glob = glob['document']

    def task_filter(task):
        return task['image-name'].match(glob)['groups'][0]['str'].match(service_conf['image-name'])

    return await conn.run(conn.db().table('tasks')
                          .filter(task_filter)
                          .order_by('id')  # should be 'time'
                          .limit(1)
                          .pluck('image-name'))


async def get_running_image(request):
    service_name = request.match_info['name']
    db_res = await get_db_running_image(service_name)
    if db_res:
        img_name = db_res[0]['image-name']
    else:
        img_name = ''
    return web.Response(status=200,
                        text=img_name,
                        content_type='text/plain')


async def get_service(request):
    pass


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

    app.router.add_get('/api/v1/documents', get_documents)
    app.router.add_post('/api/v1/documents', set_document)
    app.router.add_get('/api/v1/document/{name}', get_document)

    app.router.add_get('/api/v1/service/image/{name}', get_running_image)
    app.router.add_get('/api/v1/service/{name}', get_service)

    web.run_app(app, host='0.0.0.0', port=5000)
