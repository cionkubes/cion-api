import asyncio
import binascii
import hashlib
import json
import os
import random
import urllib.parse
from functools import wraps

import rethinkdb as r
from aiohttp import web

import rdb_conn
from permissions.permission import perm

sessions = {}


# util funcs

# -- sessions


async def watch_user(query, token):
    with rdb_conn.conn.changes(query=query) as changes:
        async for u in changes:
            sessions[token]['user'] = u['new_val']


def validate_password(password):
    if len(password) < 8:
        return False, "Password must be at least 8 characters long"
    return True, "Password is valid"


def create_session(user):
    """
    Creates a session for the given user and returns a generated session token.

    :param user: user object of the user to create a session for
    :return: The generated session-token
    """
    token = binascii.hexlify(os.urandom(64)).decode()
    sessions[token] = {}
    sessions[token]['user'] = user

    q = r.db(rdb_conn.conn.db_name).table('users').get(user['username'])
    task = asyncio.ensure_future(watch_user(q, token))
    sessions[token]['task'] = task

    return token


def retrieve_session(request):
    """
    Retrieves the stored session object stored on the session token contained
    in the given aiohttp request object.

    :param request: The aiohttp request object that contains the session token
    :return: The session user object
    """
    token = request.headers.get('X-CSRF-Token')
    return sessions[token]


def invalidate_sessions(username):
    """
    Invalidates all sessions for the given username

    :param username: username
    """
    for token in list(sessions.keys()):
        session = sessions[token]
        if username == session['user']['username']:
            sessions.pop(token)
            session['task'].cancel()


# -- hashing

def create_hash(to_hash: str):
    """
    Creates a hash from the given string. Returns the generated hash,
    and iterations and salt used.

    :param to_hash: the string to hash
    :return: hash and salt as bytes, iterations as int
    """
    iterations = random.randint(20000, 25000)
    salt = os.urandom(32)
    hash_created = hash_str(to_hash, salt, iterations)
    return hash_created, salt, iterations


def hash_str(to_hash: str, salt, iterations):
    """
    Generates a hash from the given string with the specified salt and
    iterations.

    :param to_hash: The string to hash
    :param salt: Salt to use in the hash function
    :param iterations: number of iterations to use in the hash function
    :return:
    """
    return hashlib.pbkdf2_hmac('sha512', to_hash.encode(), salt, iterations,
                               128)


# fixme: Find out if used at all. Remove if not
def has_permission(permission_tree, path):
    """
    Returns True or False depending on if the given permission dict tree
    contains the given path.

    :param permission_tree: A dictionary tree
    :param path: A list of keys to traverse the tree with
    :return: True if the tree contains the given path list, false otherwise
    """
    node = permission_tree

    for key in path[:-1]:
        if key in node:
            node = node[key]
        else:
            return False

    return isinstance(node, list) and path[-1] in node


# -- web util funcs

def bad_creds_response():
    """
    Creates and returns a 401 http response
    :return: The generated 401  http response
    """
    return web.Response(status=401,
                        text='{"error": "Bad credentials"}',
                        content_type='application/json')


def forbidden_response(error_msg):
    """
    Creates and returns a  403 forbidden response with the given error msg
    contained in the body.

    :param error_msg: The message to put in the response
    :return: The generated 403 http response
    """
    return web.Response(status=403,
                        text='{"error": "You don\'t have the correct '
                             'permissions to perform this action. Missing: '
                             f'{error_msg.join(",")}"}}',
                        content_type='application/json')


def requires_auth(func=None, permission_expr=None):
    """
    A decorator to use on aiohttp endpoints to run authentication on all
    requests before calling the endpoint function.

    :param func: The function to wrap
    :param permission_expr: The permission expression to check
    :return: The decorated function
    """
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
            user = sessions[token]['user']
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
    """
    Creates a user object in the database for the given username, password and
    permission tree.

    :param username: Username for the user
    :param password: Password in plain-text for the user
    :param permissions: The permission-tree for the user.
    :return: Database response
    """
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

@requires_auth(permission_expr=perm('cion.user.create'))
async def api_create_user(request):
    """
    aiohttp function to create a user in the database

    :param request: aiohttp request
    :return: An aiohttp response object, 422 or 201
    """
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

    valid, msg = validate_password(password)

    if not valid:
        return web.Response(status=422,
                            text=json.dumps({"error": msg}),
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
    """
    Authenticates a user and creates a session if the username and password
    in the request object matches a database entry.

    :param request: aiohttp request object
    :return: an aiohttp response object, 401 or 200 with token generated,
        username, gravatar-url and gravatar-email
    """
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
    """
    Verifies the X-CSRF-Token contained in the request object.

    :param request: aiohttp request object
    :return: 200 if valid token, 401 otherwise
    """
    token = request.headers.get('X-CSRF-Token')
    if token in sessions:
        return web.Response(status=200)
    else:
        return bad_creds_response()


async def logout(request):
    """
    Invalidates the session of the token given in the request.

    :param request: aiohttp request object
    :return: aiohttp web response object
    """
    token = request.headers.get('X-CSRF-Token')
    session = sessions.pop(token, None)
    if session:
        session['task'].cancel()
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
