import json
import re

import rethinkdb as r
from aiohttp import web
from logzero import logger

import rdb_conn
from auth import requires_auth
from permissions.permission import perm


def validate_input(string):
    """
    Validates the given string

    :param string: string to validate
    :return: the regex match search
    """
    return re.match("^[a-zA-Z0-9_\-/]+$", string)


def url_safe_service_image(service_name, image_name):
    """
    Validates the given strings and returns an aiohttp error response if they
    do not validate.
    :param service_name: name of the service
    :param image_name: name of the image
    :return: aiohttp response if given strings do not validate
    """
    if not validate_input(service_name):
        return web.Response(status=422, text="Invalid service name")
    elif not validate_input(image_name):
        return web.Response(status=422, text="Invalid image name")


# TODO: find usage
def task_base_image_name_filter(glob, image_base_name):
    """

    :param glob:
    :param image_base_name:
    :return:
    """

    def task_filter(task):
        # 1. Capture part of image-name that should match the image-name
        # from the service conf
        # 2. Match captured group against image-name from service conf
        return task['image-name'].match(glob)['groups'][0]['str'].match(
            image_base_name)

    return task_filter


# database functions

async def db_get_service_conf(service_name):
    """
    Fetches and returns configuration for the given service name

    :param service_name: name of the service to get configuration for
    :return: service configuration dictionary
    """
    return await rdb_conn.conn.run(rdb_conn.conn.db()
                                   .table('services')
                                   .get(service_name))


async def db_create_service(service_name, environments, image_name):
    """
    Creates a service configuration in the database

    :param service_name: name of the service
    :param environments: environments for the service configuration
    :param image_name: image name for the service configuration
    :return:
    """

    data = {
        'name': service_name,
        'environments': environments,
        'image-name': image_name
    }

    return await rdb_conn.conn.run(
        rdb_conn.conn.db().table("services").insert(data)
    )


async def db_replace_service(service_name, environments, image_name):
    """
    Updates a service configuration

    :param service_name: service name of the configuration to update
    :param environments: environments to update
    :param image_name: image name to update
    :return:
    """
    data = {
        'environments': environments,
        'image-name': image_name
    }

    return await rdb_conn.conn.run(
        rdb_conn.conn.db().table("services").get(service_name).update(data)
    )


async def db_get_running_image(service_name):
    """
    Gets the last updated image for a given service name.

    :param service_name: name of the service to get image for
    :return: running image
    """
    db_res = await rdb_conn.conn.run(
        rdb_conn.conn.db().table('tasks')  # All tasks
            # filter for update-tasks that were completed successfully
            .filter({'status': 'done',
                     'event': 'service-update',
                     'service': service_name})
            .group('environment')  # group by environment
            .order_by(r.desc('time'))  # Sort tasks by time
            .limit(1)  # Select only newest tasks
            .pluck('image-name', 'time')  # only return image-name and time
    )

    return {key: val.get("0", None) for key, val in db_res.items()}


async def db_get_unique_deployed_images(service_name):
    return await rdb_conn.conn.run(rdb_conn.conn.db().table('tasks')
                                   .filter({'event': 'service-update',
                                            'service-name': service_name})
                                   .pluck('image-name')
                                   .distinct()
                                   .order_by(r.desc('image-name'))
                                   .map(lambda task: task['image-name'])
                                   )


async def db_get_services():
    """
    Fetches all service configurations

    :return: service configurations
    """
    return await rdb_conn.conn.list(rdb_conn.conn.db()
                                     .table('services')
                                     )


async def db_delete_service(service_name):
    """
    Deletes a service configuration from the database

    :param service_name: name of service to delete service configuration for
    :return: database result
    """
    return await rdb_conn.conn.run(rdb_conn.conn.db()
        .table('services')
        .get(service_name)
        .delete())


# -- web request functions --

@requires_auth
async def get_services(request):
    """
    aiohttp endpoint to fetch all service configuration

    :param request: aiohttp request object
    :return: service configurations
    """
    db_res = await db_get_services()
    return web.Response(status=200,
                        text=json.dumps(db_res),
                        content_type='application/json')


@requires_auth
async def get_running_image(request):
    """
    aiohttp endpoint to get the running image of a configured service
    :param request: aiohttp request object
    :return: aiohttp response with image name
    """
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
    """
    aiohttp endpoint to fetch a service configuration

    :return: service configuration for the service name contained in the
        request
    """
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
    """
    Permission placeholder resolver for ``create_service``

    :param request: aiohttp request object
    :return: a dictionary containing environments from the request object
    """
    bod = await request.json()
    return {'env': bod['environments']}


@requires_auth(permission_expr=perm('$env.service.create',
                                    resolve_service_create))
async def create_service(request):
    """
    aiohttp endpoint to create a service configuration
    """
    bod = await request.json()

    envs = bod['environments']
    name = bod['service-name']
    image_name = bod['image-name']

    # error = url_safe_service_image(name, image_name)
    # if error:
    #     return error

    db_res = await db_create_service(name, envs, image_name)
    return web.Response(status=201, text=json.dumps(db_res))


@requires_auth
async def edit_service(request):
    """
    aiohttp endpoint to edit a service configuration
    """
    bod = await request.json()
    envs = bod['environments']
    name = bod['service-name']
    image_name = bod['image-name']

    error = url_safe_service_image(name, image_name)
    if error:
        return error

    db_res = await db_replace_service(name, envs, image_name)
    return web.Response(status=200, text=json.dumps(db_res))


async def resolve_service_delete(request):
    """
    Permission placeholder resolver for the service delete endpoint

    :param request: aiohttp request object
    :return: dictionary containing placeholder values from the request object
    """
    service_name = request.match_info['name']
    srvc_conf = await db_get_service_conf(service_name)
    print(srvc_conf)
    return {'env': srvc_conf['environments']}


@requires_auth(permission_expr=perm('$env.service.delete',
                                    resolve_service_delete))
async def delete_service(request):
    """
    aiohttp endpoint to delete a service configuration
    """
    service_name = request.match_info['name']

    db_res = await db_delete_service(service_name)
    return web.Response(status=200,
                        text=json.dumps(db_res),
                        content_type='application/json')
