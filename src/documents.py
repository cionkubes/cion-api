import json
import re

from aiohttp import web

import rdb_conn


async def db_get_document(doc_name):
    return await rdb_conn.conn.run(rdb_conn.conn.db().table('documents').get(doc_name))


# -- web request functions --

async def get_document(request):
    doc = await db_get_document(request.match_info['name'])
    return web.Response(status=200,
                        text=json.dumps(doc),
                        content_type='application/json')


async def set_document(request):
    bod = await request.json()
    await rdb_conn.conn.run(rdb_conn.conn.db().table('documents').get(bod['name']).replace(bod))
    return web.Response(status=201)


async def get_documents(request):
    documents = rdb_conn.conn.run_iter(rdb_conn.conn.db()
                                       .table('documents', read_mode='majority')
                                       .filter({'plaintext-editable': True}))

    res = []
    async for document in documents:
        res.append(document)

    return web.Response(status=200,
                        text=json.dumps(res),
                        content_type='application/json')
