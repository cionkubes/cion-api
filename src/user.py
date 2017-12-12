import binascii
import hashlib
import json
import os
import random

from aiohttp import web

import rdb_conn

resp_bad_creds = web.Response(status=401,
                              text=json.dumps({'reason': 'Bad credentials.'}),
                              content_type='application/json')

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


async def api_create_user(request):
    bod = await request.json()
    username = bod['username']
    pw_hash, salt, iterations = create_hash(bod['password'])

    db_res = await rdb_conn.conn.run(rdb_conn.conn.db().table('users').insert({
        "username": username,
        "password_hash": pw_hash,
        "salt": salt,
        "iterations": iterations
    }))

    print(db_res)

    return web.Response(status=201)


async def api_auth(request):
    bod = await request.json()
    username = bod['username']
    password = bod['password']

    user = await rdb_conn.conn.run(rdb_conn.conn.db().table('users').get(username))

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
