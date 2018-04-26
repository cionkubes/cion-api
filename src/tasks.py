import json

import rethinkdb as r
from aiohttp import web

import rdb_conn
import table
from auth import requires_auth
from permissions.permission import perm


# -- db request functions --

async def db_create_task(image, environment, service_name):
    """
    Creates a task in the database

    :param image: image-name field
    :param environment: environment field
    :param service_name: service-name field
    :return: database result
    """
    data = {
        'image-name': image,
        'event': 'service-update',
        'service-name': service_name,
        'status': 'ready',
        'environment': environment,
        'time': r.now().to_epoch_time()
    }

    return await rdb_conn.conn.run(r.db('cion').table('tasks').insert(data))


async def db_cf_tasks_status_counts():
    return await rdb_conn.conn.run(
        r.db('cion').table('tasks').changes().merge(
                r.db("cion").table("tasks")
                    .filter({"event": "service-update"})
                    .group("status")
                    .count()
                    .ungroup()
        )
    )


# -- web request functions --

async def resolve_task_create(request):
    """
    Permission placeholder resolver function for the task create endpoint

    :param request: aiohttp request object
    :return: dictionary containing values for the placeholders in the
        permission path
    """

    bod = await request.json()
    return {'env': bod['environment']}


@requires_auth(permission_expr=perm('$env.service.deploy',
                                    resolve_task_create))
async def create_task(request):
    """
    aiohttp endpoint to create a task in the database
    """
    bod = await request.json()
    db_res = await db_create_task(bod['image-name'], bod['environment'],
                                  bod['service-name'])
    return web.Response(status=200,
                        text=json.dumps(db_res),
                        content_type='application/json')


@requires_auth(permission_expr=perm('cion.view.events'))
async def get_recent_tasks(request):
    amount = int(request.query['amount'])
    result = await rdb_conn.conn.run(
        rdb_conn.conn.db().table('tasks')
            .order_by(index=r.desc('time'))
            .filter(r.row["event"] != 'log')
            .limit(amount)
            .coerce_to('array')
    )

    return web.Response(status=200,
                        text=json.dumps({'rows': result}),
                        content_type='application/json')


@requires_auth(permission_expr=perm('cion.view.events'))
async def get_task(request):
    task_id = request.match_info['id']

    result = await rdb_conn.conn.run(
        rdb_conn.conn.db().table('tasks').get(task_id))

    return web.Response(status=200,
                        text=json.dumps(result),
                        content_type='application/json')


@requires_auth(permission_expr=perm('cion.view.events'))
async def get_tasks(request):
    """
    Gets tasks from the database using the following query params:

    - pageStart: starting page
    - pageLength: length of page
    - sortIndex: index to sort by
    - searchTerm: lucene search term to filter the query by
    """
    response = await table.table_query(request, 'tasks')
    if 'web-response' in response:
        return response['web-response']
    else:
        result = response['result']
        count = response['count']

    return web.Response(status=200,
                        text=json.dumps({'rows': result,
                                         'totalLength': count}),
                        content_type='application/json')
