import json

import rethinkdb as r
from aiohttp import web

import auth
import rdb_conn
from auth import requires_auth


async def db_set_gravatar_email(username, gravatar_email):
    db_res = await rdb_conn.conn.run(
        rdb_conn.conn.db().table('users').get(username).update({
            "gravatar-email": gravatar_email
        }))

    return db_res


async def db_get_users():
    return await rdb_conn.conn.run(rdb_conn.conn.db()
                                   .table('users')
                                   .pluck('username', 'time_created')
                                   .order_by(r.desc('username'))
                                   )


async def db_change_password(username, password):
    pw_hash, salt, iters = auth.create_hash(password)
    return await rdb_conn.conn.run(
        rdb_conn.conn.db().table('users').get(username).update({
            'password_hash': pw_hash,
            'salt': salt,
            'iterations': iters
        })
    )


async def db_delete_user(username):
    return await rdb_conn.conn.run(
        rdb_conn.conn.db().table('users').get(username).delete()
    )


def bad_creds_response():
    return web.Response(status=401,
                        text='{"error": "Bad credentials."}',
                        content_type='application/json')


@requires_auth
async def set_gravatar_email(request):
    bod = await request.json()
    email = bod['gravatar-email']

    key = request.headers.get('X-CSRF-Token')

    db_res = await db_set_gravatar_email(auth.sessions.get(key)['username'],
                                         email)

    if 'errors' in db_res and db_res['errors']:
        return web.Response(status=422,
                            text=json.dumps({
                                'message': 'Error inserting '
                                           'into the '
                                           'database'}),
                            content_type='application/json')

    return web.Response(status=200,
                        text=json.dumps(
                            {'message': 'gravatar-email was updated'}),
                        content_type='application/json')


@requires_auth
async def get_users(request):
    db_res = await db_get_users()

    if 'errors' in db_res and db_res['errors']:
        return web.Response(status=422,
                            text=json.dumps({
                                'message': 'Error getting '
                                           'users from the '
                                           'database'}),
                            content_type='application/json')

    return web.Response(status=200,
                        text=json.dumps(db_res),
                        content_type='application/json')


@requires_auth
async def change_password(request):
    bod = await request.json()
    pw = bod['new-password']
    pw_rep = bod['repeat-password']
    if not pw == pw_rep:
        return web.Response(status=422,
                            text=json.dumps(
                                {'error': 'passwords do not match'}),
                            content_type='application/json')

    username = request.match_info['name']
    db_res = await db_change_password(username, pw)
    if 'errors' in db_res and db_res['errors']:
        return web.Response(status=422,
                            text=json.dumps({
                                'message': 'Error setting password'}),
                            content_type='application/json')

    return web.Response(status=200,
                        text=json.dumps(db_res),
                        content_type='application/json')


@requires_auth
async def delete_user(request):
    username = request.match_info['name']
    db_res = await db_delete_user(username)

    if 'errors' in db_res and db_res['errors']:
        return web.Response(status=422,
                            text=json.dumps({
                                'message': 'Error deleting user'}),
                            content_type='application/json')

    return web.Response(status=200,
                        text=json.dumps(db_res),
                        content_type='application/json')