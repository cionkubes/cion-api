import json

import luqum.parser
import rethinkdb as r
from aiohttp import web

import rdb_conn
import search
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
        'event': 'update-service',
        'service-name': service_name,
        'status': 'ready',
        'environment': environment,
        'time': r.now().to_epoch_time()
    }

    return await rdb_conn.conn.run(r.db('cion').table('tasks').insert(data))


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
async def get_tasks(request):
    """
    Gets tasks from the database using the following query params:

    - pageStart: starting page
    - pageLength: length of page
    - sortIndex: index to sort by
    - searchTerm: lucene search term to filter the query by
    """
    page_start = int(request.query['pageStart'])
    page_length = int(request.query['pageLength'])
    sort_index = request.query['sortIndex']
    if sort_index == '-1':
        sort_index = 'time'
    search_term = request.query['searchTerm']

    sort_direction = r.asc \
        if request.query['reverseSort'].lower() == 'true' \
        else r.desc

    print(page_start, page_start + page_length)

    filter_func = search.get_filter(search_term)
    try:
        if not filter_func:
            result = await rdb_conn.conn.run(
                rdb_conn.conn.db().table('tasks')
                    .order_by(index=sort_direction(sort_index))
                    .slice(page_start, page_start + page_length)
                    .coerce_to('array')
            )

            count = await rdb_conn.conn.run(
                rdb_conn.conn.db().table('tasks').count())
        else:
            try:
                db_res = await rdb_conn.conn.run(
                    rdb_conn.conn.db().table('tasks')
                        .order_by(index=sort_direction(sort_index))
                        .filter(filter_func)
                        .coerce_to('array')
                        .do(lambda res: {
                        'result': res.slice(page_start,
                                            page_start + page_length),
                        'length': res.count()
                    })
                )

                result = db_res['result']
                count = db_res['length']
            except r.errors.ReqlResourceLimitError as e:
                return web.Response(status=400,
                                    text=json.dumps({
                                        'error': 'Unable to sort on given '
                                                 'index. Too many results to '
                                                 'sort, due to that the '
                                                 'requested index has not '
                                                 'been created in the '
                                                 'database.'}),
                                    content_type='application/json')

    except luqum.parser.ParseError as e:
        result = []
        count = 0

    return web.Response(status=200,
                        text=json.dumps({'rows': result,
                                         'totalLength': count}),
                        content_type='application/json')
