import json as jsonlib

from aiohttp import web
from logzero import logger

import rdb_conn
import functools
import asyncio

from auth import requires_auth
from permissions.permission import perm

def lazy(fn):
    """
    Decorator that ensures the function is only 
    called once, the result is cached and reused 
    on successive calls.

    :param fn: Function to decorate
    :return: The lazy function
    """
    functools.wraps(fn)

    value = None
    def wrapper():
        nonlocal value
        if value is None:
            value = fn()

        return value

    return wrapper


@lazy
def editable_documents():
    documents = []

    with open('default_docs.json') as file:
        for table in jsonlib.load(file):
            if 'editable' in table and table['editable']:
                documents.append(table['name'])

    return documents


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


def db_get_document(doc_name, **kwargs):
    """
    Gets and returns the document by the given name from the database.

    :param doc_name: Name of the document to fetch
    :return: The fetched document
    """
    return rdb_conn.conn.db().table(doc_name, **kwargs)


def generate_permission_def(swarms):
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

    for swarm in swarms:
        perms[swarm['name']] = env_perms

    sort_array_values(perms)
    return perms


# -- web request functions --

@requires_auth
async def get_document(request):
    """
    Aiohttp endpoint to fetch a document from the database
    :param request: aiohttp request object
    :return: the fetched document in an aiohttp request object
    """
    doc = await rdb_conn.conn.list(db_get_document(request.match_info['name']))
    return json(doc)


@requires_auth(permission_expr=perm('cion.config.edit'))
async def set_document(request):
    """
    Replaces a document with the given in the database with the given document
    body.

    :param request: aiohttp request object
    :return: aiohttp response object with a **200** http status code.
    """
    body = await request.json()
    name = body['name']

    await rdb_conn.conn.run(db_get_document(name).delete())
    await rdb_conn.conn.run(
        db_get_document(name).insert(body['document'])
    )

    return json({"message": "Successfully saved document"}, status=201)


@requires_auth
async def get_documents(request):
    """
    Aiohttp endpoint to fetch all documents from the database who have
    ``plaintext-editable`` set to true.

    :param request: aiohttp request object
    :return: aiohttp response object
    """

    async def document(name):
        return name, await rdb_conn.conn.list(db_get_document(name, read_mode='majority'))

    async def documents():
        for doc_name in editable_documents():
            yield await document(doc_name)

    res = []
    async for name, doc in documents():
        res.append({"name": name, "document": doc})

    return json(res)


@requires_auth
async def get_permission_def(request):
    """
    Aiohttp endpoint to fetch the definition for the permission tree.

    :param request: aiohttp request
    :return: aiohttp response with permission definition tree in body
    """

    environments = \
        await rdb_conn.conn.list(rdb_conn.conn.db().table('environments'))
    return json(generate_permission_def(environments), sort_keys=True)


def json(data, status=200, **kwargs):
    """
    Creates a json webresponse

    :param status: HTTP status code of response
    :param data: json data to respond with
    :param kwargs: arguments to json.dumps
    :return: Web response
    """
    return web.Response(status=status,
                        text=jsonlib.dumps(data, **kwargs),
                        content_type='application/json')
