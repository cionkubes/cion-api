import json
import re

from aiohttp import web

import rdb_conn

# -- web request functions --


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
