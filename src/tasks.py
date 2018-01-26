import json

import rethinkdb as r
from aiohttp import web

import rdb_conn

# -- db request functions --
from auth import requires_auth


async def db_create_task(image, environment, service_name):
    data = {
        'image-name': image,
        'event': 'update-service',
        'service-name': service_name,
        'status': 'ready',
        'environment': environment,
        'time': r.now().to_epoch_time()
    }

    return await rdb_conn.conn.run(r.db('cion').table('tasks').insert(data))


# -- web request functions --

@requires_auth
async def create_task(request):
    bod = await request.json()
    db_res = await db_create_task(bod['image-name'], bod['environment'],
                                  bod['service-name'])
    return web.Response(status=200,
                        text=json.dumps(db_res),
                        content_type='application/json')


@requires_auth
async def get_tasks(request):
    db_res = await rdb_conn.conn.run(rdb_conn.conn.db().table('tasks')
                                     .filter(request.match_info['event'])
                                     .order_by(r.desc('time'))
                                     )
    return web.Response(status=200,
                        text=json.dumps(db_res),
                        content_type='application/json')
