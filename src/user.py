import json

import rethinkdb as r
from aiohttp import web

import auth
import rdb_conn
from auth import requires_auth
from permissions.permission import perm


async def db_set_gravatar_email(username, gravatar_email):
    """
    Sets the gravatar field for the given user to the given value in the
    database
    """
    db_res = await rdb_conn.conn.run(
        rdb_conn.conn.db().table('users').get(username).update({
            "gravatar-email": gravatar_email
        }))

    return db_res


async def db_get_users():
    """
    Gets all users from the dictionary
    """
    return await rdb_conn.conn.run(rdb_conn.conn.db()
                                   .table('users')
                                   .pluck('username', 'time_created')
                                   .order_by(r.desc('username'))
                                   )


async def db_change_password(username, password):
    """
    Sets the password for the given username to the given value

    :param username: username
    :param password: plain-text password to generate hash-information for
    :return: database result
    """
    pw_hash, salt, iters = auth.create_hash(password)
    return await rdb_conn.conn.run(
        rdb_conn.conn.db().table('users').get(username).update({
            'password_hash': pw_hash,
            'salt': salt,
            'iterations': iters
        })
    )


async def db_delete_user(username):
    """
    Deletes a user from the database

    :param username: username for the user to delete
    :return: database result
    """
    return await rdb_conn.conn.run(
        rdb_conn.conn.db().table('users').get(username).delete()
    )


async def db_get_permissions(username):
    """
    Fetches the permission tree for the given username

    :param username: username to fetch permissions for
    :return: database result
    """
    return await rdb_conn.conn.run(
        rdb_conn.conn.db().table('users').get(username).pluck('permissions')
    )


async def db_set_permissions(username, permissions):
    """
    Sets the permission tree for the given username to the given
    permission tree.

    :param username: username for the user to update
    :param permissions: new value for the permission-tree
    :return: database result
    """
    return await rdb_conn.conn.run(
        rdb_conn.conn.db().table('users').get(username).update(
            {'permissions': r.literal(permissions)})
    )


# -- web endpoint funcs

def bad_creds_response():
    """
    Generates an aiohttp response with http status code 401

    :return: the generated response object
    """
    return web.Response(status=401,
                        text='{"error": "Bad credentials."}',
                        content_type='application/json')


@requires_auth
async def get_permissions(request):
    """
    aiohttp endpoint to fetch a user's permission tree

    The username comes from a path parameter in the request url
    """
    username = request.match_info['username']
    permissions = await db_get_permissions(username)

    if 'errors' in permissions and permissions['errors']:
        return web.Response(status=404,
                            text=json.dumps({
                                'message': 'No user with that username '
                                           'exists or other database error '
                                           'occured'}),
                            content_type='application/json')

    return web.Response(status=200,
                        text=json.dumps(permissions),
                        content_type='application/json')


@requires_auth(permission_expr=perm('cion.user.edit'))
async def set_permissions(request):
    """
    Updates the permission-tree for a user.

    Username comes from a path-parameter. Permission tree comes from the
    request body.
    """
    username = request.match_info['username']
    bod = await request.json()
    permissions = bod['permissions']

    db_res = await db_set_permissions(username, permissions)

    if 'errors' in db_res and db_res['errors']:
        return web.Response(status=404,
                            text=json.dumps({
                                'message': 'No user with that username '
                                           'exists or other database error '
                                           'occured'}),
                            content_type='application/json')

    return web.Response(status=200,
                        text=json.dumps(
                            {'message': 'Permissions successfully updates'}),
                        content_type='application/json')


@requires_auth
async def set_gravatar_email(request):
    """
    aiohttp endpoint to update the gravatar email field in the database for the
    currently logged in user.

    Gravatar value comes from the body
    """
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
    """
    aiohttp endpoint to fetch all users from the database
    """
    db_res = await db_get_users()

    if 'errors' in db_res and db_res['errors']:
        return web.Response(status=422,
                            text=json.dumps({
                                'error': 'Error getting users from the '
                                         'database'}),
                            content_type='application/json')

    return web.Response(status=200,
                        text=json.dumps(db_res),
                        content_type='application/json')


@requires_auth
async def change_password(request):
    """
    Updates the password for the specified user.

    Username comes from the request url.
    """
    bod = await request.json()
    pw = bod['new-password']
    pw_rep = bod['repeat-password']
    if not pw == pw_rep:
        return web.Response(status=422,
                            text=json.dumps(
                                {'error': 'Passwords do not match'}),
                            content_type='application/json')

    username = request.match_info['name']
    db_res = await db_change_password(username, pw)
    if 'errors' in db_res and db_res['errors']:
        return web.Response(status=422,
                            text=json.dumps({
                                'error': 'Error setting password'}),
                            content_type='application/json')

    return web.Response(status=200,
                        text=json.dumps(db_res),
                        content_type='application/json')


@requires_auth(permission_expr=perm('cion.user.delete'))
async def delete_user(request):
    """
    aiohttp endpoint to delete a user from the database.

    Username comes from a path parameter in the request url.
    """
    username = request.match_info['name']

    if username == "admin":
        return web.Response(status=422,
                            text=json.dumps({
                                'error': 'You cannot delete the admin user'}),
                            content_type='application/json')

    db_res = await db_delete_user(username)

    if 'errors' in db_res and db_res['errors']:
        return web.Response(status=422,
                            text=json.dumps({
                                'error': 'Error deleting user'}),
                            content_type='application/json')

    return web.Response(status=200,
                        text=json.dumps(db_res),
                        content_type='application/json')
