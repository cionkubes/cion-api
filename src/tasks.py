import json

import rethinkdb
from aiohttp import web

import documents
import rdb_conn
import rethinkdb as r


# -- db request functions --

def task_base_image_name_filter(glob, image_base_name):
    def task_filter(task):
        # 1. Capture part of image-name that should match the image-name from the service conf
        # 2. Match captured group against image-name from service conf
        return task['image-name'].match(glob)['groups'][0]['str'].match(image_base_name)

    return task_filter


async def db_get_running_image(service_name, environment=None, service_conf=None, glob=None):  # TODO: refactor
    if not service_conf:
        service_conf = await documents.db_get_service_conf(service_name)

    if not service_conf:
        return ''

    if not glob:
        glob = (await documents.db_get_document('glob'))['document']

    image_base_name = service_conf['image-name']

    task_filter = task_base_image_name_filter(glob, image_base_name)

    db_res = await rdb_conn.conn.run(rdb_conn.conn.db().table('tasks')  # All tasks
                                     .filter({'status': 'done'})  # filter for tasks that were completed successfully
                                     .filter(task_filter)  # Filter by image-name
                                     .group('environment')  # group by environment
                                     .order_by(r.desc('time'))  # Sort each group by time
                                     .limit(1)  # Top result from each group
                                     .pluck('image-name', 'time')  # only return image-name and time
                                     )
    return {key: val[0] for key, val in db_res.items()}


async def db_get_unique_deployed_images(service_name, service_conf=None, glob=None):
    if not service_conf:
        service_conf = await documents.db_get_service_conf(service_name)

    if not service_conf:
        return ''

    if not glob:
        glob = (await documents.db_get_document('glob'))['document']

    image_base_name = service_conf['image-name']
    task_filter = task_base_image_name_filter(glob, image_base_name)

    db_res = await rdb_conn.conn.run(rdb_conn.conn.db().table('tasks')
                                     .pluck('image-name')
                                     .distinct()
                                     .filter(task_filter)
                                     .order_by(r.desc('image-name'))
                                     .map(lambda task: task['image-name'])
                                     )
    return db_res


# -- web request functions --

async def get_tasks(request):
    db_res = await rdb_conn.conn.run(rdb_conn.conn.db().table('tasks').order_by(r.desc('time')))
    return web.Response(status=200,
                        text=json.dumps(db_res),
                        content_type='application/json')


async def get_running_image(request):
    service_name = request.match_info['name']
    db_res = await db_get_running_image(service_name)
    print(db_res)
    if db_res:
        img_name = db_res['image-name']
    else:
        img_name = ''
    return web.Response(status=200,
                        text=json.dumps(img_name),
                        content_type='application/json')
