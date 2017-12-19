import json
import re

from aiohttp import web

import rdb_conn


# -- db request functions --

async def image_and_tag(full_image):
    glob = await db_get_document('glob')
    match = re.match(glob, full_image)
    return match.group(0), match.group(1)


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
    db_res = await rdb_conn.conn.run(rdb_conn.conn.db().table('documents').get(bod['name']).replace(bod))
    return web.Response(status=201)


async def get_documents(request):
    t = []
    async for document in rdb_conn.conn.run_iter(rdb_conn.conn.db().table('documents', read_mode='majority')):
        t.append(document)
    return web.Response(status=200,
                        text=json.dumps(t),
                        content_type='application/json')
