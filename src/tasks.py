import json

from aiohttp import web

import documents
import rdb_conn


# -- db request functions --

async def db_get_running_image(service_name, service_conf=None, glob=None):  # TODO: refactor
    if not service_conf:
        service_conf = await documents.db_get_service_conf(service_name)

    if not service_conf:
        return ''

    if not glob:
        glob = (await documents.db_get_document('glob'))['document']

    def task_filter(task):
        # 1. Capture part of image-name that should match the image-name from the service conf
        # 2. Match captured group against image-name from service conf
        return task['image-name'].match(glob)['groups'][0]['str'].match(service_conf['image-name'])

    # TODO: filter for both status and environment and return for all environments given in service_conf
    db_res = await rdb_conn.conn.run(rdb_conn.conn.db().table('tasks')  # All tasks
                                     .filter(task_filter)  # Filter by image-name
                                     .order_by('id')  # Sort by id. TODO: Should be 'time'
                                     .limit(1)  # Top result
                                     .pluck('image-name'))  # Return only the field 'image-name'

    return db_res[0] if db_res else {}


# -- web request functions --

async def get_tasks(request):
    t = []
    async for task in rdb_conn.conn.run_iter(rdb_conn.conn.db().table('tasks', read_mode='majority')):
        t.append(task)

    return web.Response(status=200,
                        text=json.dumps(t),
                        content_type='application/json')


async def get_running_image(request):
    service_name = request.match_info['name']
    db_res = await db_get_running_image(service_name)
    if db_res:
        img_name = db_res['image-name']
    else:
        img_name = ''
    return web.Response(status=200,
                        text=img_name,
                        content_type='text/plain')
