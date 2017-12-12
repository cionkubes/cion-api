import json
import re

from aiohttp import web

import rdb_conn

# -- db request functions --
import tasks


async def image_and_tag(full_image):
    glob = await db_get_document('glob')
    match = re.match(glob, full_image)
    return match.group(0), match.group(1)


async def db_get_document(doc_name):
    return await rdb_conn.conn.run(rdb_conn.conn.db().table('documents').get(doc_name))


async def db_get_service_conf(service_name):
    db_res = await rdb_conn.conn.run(rdb_conn.conn.db().table('documents').get('images')['document'])
    if service_name in db_res:
        return db_res[service_name]
    else:
        return {}


# -- web request functions --

async def get_service(request):
    service_name = request.match_info['name']
    service_conf = await db_get_service_conf(service_name)
    print(service_name)
    print(service_conf)

    glob = (await db_get_document('glob'))['document']
    data = {
        'environments': {
            k: (await tasks.db_get_running_image(service_name, service_conf, glob))['image-name']
            for k in service_conf['environments']
        }
    }

    print(data)

    return web.Response(status=200,
                        text=json.dumps(data),
                        content_type='application/json')


async def get_document(request):
    doc = await db_get_document(request.match_info['name'])
    return web.Response(status=200,
                        text=json.dumps(doc),
                        content_type='application/json')


async def set_document(request):
    bod = await request.json()
    db_res = await rdb_conn.conn.run(rdb_conn.conn.db().table('documents').get(bod['name']).replace(bod))
    print(db_res)
    return web.Response(status=201)


async def get_documents(request):
    t = []
    async for document in rdb_conn.conn.run_iter(rdb_conn.conn.db().table('documents', read_mode='majority')):
        t.append(document)
    return web.Response(status=200,
                        text=json.dumps(t),
                        content_type='application/json')
