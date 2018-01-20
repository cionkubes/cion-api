import binascii
import hashlib
import json
import os
import random
import urllib.parse
from functools import wraps

from aiohttp import web

import rdb_conn

sessions = {}


async def db_create_user(username, password):
    pw_hash, salt, iterations = create_hash(password)

    db_res = await rdb_conn.conn.run(rdb_conn.conn.db().table('users').insert({
        "username": username,
        "password_hash": pw_hash,
        "salt": salt,
        "iterations": iterations
    }))

    return db_res


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


def bad_creds_response():
    return web.Response(status=401,
                        text='{"error": "Bad credentials."}',
                        content_type='application/json')


def requires_auth(f):
    @wraps(f)
    async def wrapper(request):
        token = request.headers.get('X-CSRF-Token')
        if token not in sessions:
            return bad_creds_response()
        return await f(request)

    return wrapper


@requires_auth
async def api_create_user(request):
    bod = await request.json()
    username = bod['username']

    if not username:
        return web.Response(status=422, text='{"error": "Username cannot be empty"}')

    password = bod['password']

    if not password:
        return web.Response(status=422, text='{"error": "Password cannot be empty"}')

    db_res = await db_create_user(username, password)

    if 'errors' in db_res and db_res['errors']:
        if db_res['first_error'].find('Duplicate primary key') > -1:
            text = f'User \'{username}\' already exists'
        else:
            text = 'Something went wrong when inserting in database'
        return web.Response(status=422, text=json.dumps({'error': text}), content_type='application/json')

    return web.Response(status=201, text=json.dumps(db_res), content_type='application/json')


async def api_auth(request):
    bod = await request.json()
    username = bod['username']
    password = bod['password']
    user = await rdb_conn.conn.run(rdb_conn.conn.db().table('users').get(username))

    if not user:
        return bad_creds_response()

    salt = user['salt']
    iterations = user['iterations']
    stored_hash = user['password_hash']
    input_hash = hash_str(password, salt, iterations)
    if not input_hash == stored_hash:
        print('password incorrect')
        return bad_creds_response()

    token = create_session(user)

    if 'gravatar-email' not in user or not user['gravatar-email']:
        gravatar_email = ''
        gravatar_base = username
    else:
        gravatar_base = user['gravatar-email']
        gravatar_email = gravatar_base

    gravatar_base = gravatar_base.lower().encode('utf-8')

    gravatar_url = "https://www.gravatar.com/avatar/" + hashlib.md5(gravatar_base).hexdigest() + "?"
    gravatar_url += urllib.parse.urlencode({'d': 'identicon', 's': str(200)})  # TODO: dynamic size

    return web.Response(status=200,
                        text=json.dumps({
                            'token': token,
                            'user': {
                                'username': user['username'],
                                'gravatar-url': gravatar_url,
                                'gravatar-email': gravatar_email
                            }
                        }),
                        content_type='application/json')


async def logout(request):
    token = request.headers.get('X-CSRF-Token')
    user = sessions.pop(token, False)
    if user:
        return web.Response(status=200,
                            text=json.dumps({'message': 'session popped; user was logged out'}),
                            content_type='application/json')
    else:
        return web.Response(status=200,
                            text=json.dumps({'message': 'token does not exist, so user has no session'}),
                            content_type='application/json')
