import binascii
import hashlib
import json
import os
import random
import urllib.parse
from functools import wraps

from permissions.permission import perm

import rethinkdb as r
from aiohttp import web

import rdb_conn

sessions = {}


# util funcs

# -- sessions

def create_session(user):
    token = binascii.hexlify(os.urandom(64)).decode()
    sessions[token] = user
    return token


def retrieve_session(request):
    token = request.headers.get('X-CSRF-Token')
    return sessions[token]


# -- hashing

def create_hash(to_hash: str):
    iterations = random.randint(20000, 25000)
    salt = os.urandom(32)
    hash_created = hash_str(to_hash, salt, iterations)
    return hash_created, salt, iterations


def hash_str(to_hash: str, salt, iterations):
    return hashlib.pbkdf2_hmac('sha512', to_hash.encode(), salt, iterations,
                               128)


def has_permission(permission_tree, path):
    node = permission_tree

    for key in path[:-1]:
        if key in node:
            node = node[key]
        else:
            return False

    return isinstance(node, list) and path[-1] in node


# -- web util funcs

def bad_creds_response():
    return web.Response(status=401,
                        text='{"error": "Bad credentials"}',
                        content_type='application/json')


def forbidden_response(error_msg):
    return web.Response(status=403,
                        text='{"error": "You don\'t have the correct '
                             'permissions to perform this action. Missing: '
                             f'{error_msg.join(",")}"}}',
                        content_type='application/json')


def requires_auth(func=None, permission_expr=None):
    if func:
        permission_expr = None

    error_msg = ""

    def error_fn(error_reason):
        nonlocal error_msg
        error_msg = error_reason

    def decorator(f):
        @wraps(f)
        async def wrapper(request):
            token = request.headers.get('X-CSRF-Token')
            if token not in sessions:
                return bad_creds_response()
            user = sessions[token]
            if permission_expr \
                    and ('permissions' not in user
                         or not await permission_expr.has_permission(
                            user['permissions'], error_fn, request)):
                return forbidden_response(error_msg)
            return await f(request)

        return wrapper

    return decorator(func) if func else decorator


# database funcs

async def db_create_user(username, password, permissions):
    pw_hash, salt, iterations = create_hash(password)

    db_res = await rdb_conn.conn.run(rdb_conn.conn.db().table('users').insert({
        "username": username,
        "password_hash": pw_hash,
        "salt": salt,
        "iterations": iterations,
        "time_created": r.now().to_epoch_time(),
        'permissions': permissions
    }))

    return db_res


# API endpoints

@requires_auth(
    permission_expr=perm('cion.user.create') & perm('cion.user.delete'))
async def api_create_user(request):
    bod = await request.json()
    username = bod['username']

    if not username:
        return web.Response(status=422,
                            text='{"error": "Username cannot be empty"}',
                            content_type='application/json')

    username = username.lower()
    password = bod['password']

    if not password:
        return web.Response(status=422,
                            text='{"error": "Password cannot be empty"}',
                            content_type='application/json')

    if len(password) < 8:
        return web.Response(status=422,
                            text='{"error": "Password must be at least 8 '
                                 'characters long"}',
                            content_type='application/json')

    repeat_password = bod['repeat-password']
    if not repeat_password == password:
        return web.Response(status=422,
                            text='{"error": "Password and repeat-password '
                                 'must match"}',
                            content_type='application/json')

    if 'permissions' not in bod:
        permissions = {}
    else:
        permissions = bod['permissions']

    db_res = await db_create_user(username, password, permissions)

    if 'errors' in db_res and db_res['errors']:
        if db_res['first_error'].find('Duplicate primary key') > -1:
            text = f'User \'{username}\' already exists'
        else:
            text = 'Something went wrong when inserting in database'
        return web.Response(status=422,
                            text=json.dumps({'error': text}),
                            content_type='application/json')

    return web.Response(status=201,
                        text=json.dumps(db_res),
                        content_type='application/json')


async def api_auth(request):
    bod = await request.json()
    username = bod['username']
    if not username:
        return bad_creds_response()

    username = username.lower()
    password = bod['password']
    user = await rdb_conn.conn.run(rdb_conn.conn.db()
                                   .table('users')
                                   .get(username)
                                   )

    if not user:
        return bad_creds_response()

    salt = user['salt']
    iterations = user['iterations']
    stored_hash = user['password_hash']
    input_hash = hash_str(password, salt, iterations)
    if not input_hash == stored_hash:
        return bad_creds_response()

    token = create_session(user)

    if 'gravatar-email' not in user or not user['gravatar-email']:
        gravatar_email = ''
        gravatar_base = username
    else:
        gravatar_base = user['gravatar-email']
        gravatar_email = gravatar_base

    gravatar_base = gravatar_base.lower().encode('utf-8')

    gravatar_url = "https://www.gravatar.com/avatar/" \
                   + hashlib.md5(gravatar_base).hexdigest() \
                   + "?" \
                   + urllib.parse.urlencode({'d': 'identicon', 's': str(200)})
    # TODO: dynamic size

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


async def verify_token(request):
    token = request.headers.get('X-CSRF-Token')
    if token in sessions:
        return web.Response(status=200)
    else:
        return bad_creds_response()


async def logout(request):
    token = request.headers.get('X-CSRF-Token')
    user = sessions.pop(token, False)
    if user:
        return web.Response(status=200,
                            text=json.dumps({
                                'message':
                                    'session popped; user was logged out'
                            }),
                            content_type='application/json')
    else:
        return web.Response(status=200,
                            text=json.dumps({
                                'message':
                                    'token does not exist, so user has no '
                                    'session'
                            }),
                            content_type='application/json')
