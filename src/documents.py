import json

from aiohttp import web

import rdb_conn
from auth import requires_auth
from permissions.permission import perm


def sort_array_values(d):
    """
    Sorts a dict's values

    :param d: dictionary, can be a tree
    :return: The sorted dictionary
    """
    if type(d) is dict:
        for k, v in d.items():
            sort_array_values(v)
    else:
        d.sort()


async def db_get_document(doc_name):
    """
    Gets and returns the document by the given name from the database.

    :param doc_name: Name of the document to fetch
    :return: The fetched document
    """
    return await rdb_conn.conn.run(
        rdb_conn.conn.db().table('documents').get(doc_name))


# -- web request functions --

@requires_auth
async def get_document(request):
    """
    Aiohttp endpoint to fetch a document from the database
    :param request: aiohttp request object
    :return: the fetched document in an aiohttp request object
    """
    doc = await db_get_document(request.match_info['name'])
    return web.Response(status=200,
                        text=json.dumps(doc),
                        content_type='application/json')


@requires_auth(permission_expr=perm('cion.config.edit'))
async def set_document(request):
    """
    Replaces a document with the given in the database with the given document
    body.

    :param request: aiohttp request object
    :return: aiohttp response object with a **200** http status code.
    """
    bod = await request.json()
    await rdb_conn.conn.run(
        rdb_conn.conn.db().table('documents').get(bod['name']).replace(bod))
    return web.Response(status=201,
                        text='{"message": "Successfully save document"}',
                        content_type='application/json')


@requires_auth
async def get_documents(request):
    """
    Aiohttp endpoint to fetch all documents from the database who have
    ``plaintext-editable`` set to true.

    :param request: aiohttp request object
    :return: aiohttp response object
    """
    documents = rdb_conn.conn.run_iter(rdb_conn.conn.db()
                                       .table('documents',
                                              read_mode='majority')
                                       .filter({'plaintext-editable': True}))
    res = []
    async for document in documents:
        res.append(document)

    return web.Response(status=200,
                        text=json.dumps(res),
                        content_type='application/json')


@requires_auth
async def get_permission_def(request):
    """
    Aiohttp endpoint to fetch the definition for the permission tree.

    :param request: aiohttp request
    :return: aiohttp response with permission definition tree in body
    """
    perms = {
        'cion': {
            'user': ['create', 'edit', 'delete'],
            'view': ['events', 'config'],
            'config': ['edit']
        }
    }
    env_perms = {
        'service': ['create', 'edit', 'delete', 'deploy']
    }

    swarms = await db_get_document('swarms')
    for k in swarms['document'].keys():
        perms[k] = env_perms

    sort_array_values(perms)

    return web.Response(status=200,
                        text=json.dumps(perms, sort_keys=True),
                        content_type='application/json')
