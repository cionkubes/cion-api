import json

import re
import rethinkdb as r
from aiohttp import web

import rdb_conn
from auth import requires_auth
from permissions.permission import perm


def check_url_safe(string):
    return re.match("^[a-zA-Z0-9_-]+$", string)


def url_safe_service_image(service_name, image_name):
    if not check_url_safe(service_name):
        return web.Response(status=422, text="Invalid service name")
    elif not check_url_safe(image_name):
        return web.Response(status=422, text="Invalid image name")


def task_base_image_name_filter(glob, image_base_name):
    def task_filter(task):
        # 1. Capture part of image-name that should match the image-name
        # from the service conf
        # 2. Match captured group against image-name from service conf
        return task['image-name'].match(glob)['groups'][0]['str'].match(
            image_base_name)

    return task_filter


# database functions

async def db_get_service_conf(service_name):
    return await rdb_conn.conn.run(rdb_conn.conn.db()
                                   .table('documents')
                                   .get('services')['document'][service_name])


async def db_create_service(service_name, environments, image_name):
    return await db_replace_service(service_name, environments, image_name)


async def db_replace_service(service_name, environments, image_name):
    data = {
        'environments': environments,
        'image-name': image_name
    }

    return await rdb_conn.conn.run(
        rdb_conn.conn.db().table('documents').get("services").update(
            {"document": {service_name: data}}
        ))


async def db_get_running_image(service_name):
    db_res = await rdb_conn.conn.run(
        rdb_conn.conn.db().table('tasks')  # All tasks
        # filter for update-tasks that were completed successfully
        .filter({'status': 'done', 'event': 'update-service',
                 'service-name': service_name})
        .group('environment')  # group by environment
        .order_by(r.desc('time'))  # Sort tasks by time
        .limit(1)  # Select only newest tasks
        .pluck('image-name', 'time')  # only return image-name and time
        )

    return {key: val.get("0", None) for key, val in db_res.items()}


async def db_get_unique_deployed_images(service_name):
    return await rdb_conn.conn.run(rdb_conn.conn.db().table('tasks')
                                   .filter(
        {'status': 'done', 'event': 'update-service',
         'service-name': service_name})
                                   .pluck('image-name')
                                   .distinct()
                                   .order_by(r.desc('image-name'))
                                   .map(lambda task: task['image-name'])
                                   )


async def db_get_services():
    db_res = await rdb_conn.conn.run(rdb_conn.conn.db()
                                     .table('documents')
                                     .get('services')['document'])

    return [{"name": name, **service} for name, service in db_res.items()]


async def db_delete_service(service_name):
    return await rdb_conn.conn.run(rdb_conn.conn.db().table('documents')
                                   .get('services')
                                   .replace(
        r.row.without({'document': {service_name: True}})))


# -- web request functions --

@requires_auth
async def get_services(request):
    db_res = await db_get_services()
    return web.Response(status=200,
                        text=json.dumps(db_res),
                        content_type='application/json')


@requires_auth
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


@requires_auth
async def get_service(request):
    service_name = request.match_info['name']
    service_conf = await db_get_service_conf(service_name)

    if not service_conf:
        return web.Response(status=404,
                            text='{"error": "Service is not configured"}',
                            content_type='application/json')

    envs = {}
    db_res = await db_get_running_image(service_name)
    for env in sorted(service_conf['environments']):
        if env not in db_res:
            envs[env] = {'image-name': 'NA', 'time': None}
        else:
            envs[env] = db_res[env]

    data = {
        'environments': envs,
        'images-deployed': await db_get_unique_deployed_images(service_name)
    }
    return web.Response(status=200,
                        text=json.dumps(data),
                        content_type='application/json')


async def resolve_service_create(request):
    bod = await request.json()
    return {'env': bod['environments']}


@requires_auth(permission_expr=perm('$env.service.create',
                                    resolve_service_create))
async def create_service(request):
    bod = await request.json()

    envs = bod['environments']
    name = bod['service-name']
    image_name = bod['image-name']

    error = url_safe_service_image(name, image_name)
    if error:
        return error

    db_res = await db_create_service(name, envs, image_name)
    return web.Response(status=201, text=json.dumps(db_res))


@requires_auth
async def edit_service(request):
    bod = await request.json()
    envs = bod['environments']
    name = bod['service-name']
    image_name = bod['image-name']

    error = url_safe_service_image(name, image_name)
    if error:
        return error

    db_res = await db_replace_service(name, envs, image_name)
    return web.Response(status=200, text=json.dumps(db_res))


@requires_auth
async def delete_service(request):
    service_name = request.match_info['name']
    db_res = await db_delete_service(service_name)
    return web.Response(status=200,
                        text=json.dumps(db_res),
                        content_type='application/json')
