import json

import luqum.parser
import rethinkdb as r
from aiohttp import web

import search

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
    page_start = int(request.query['pageStart'])
    page_length = int(request.query['pageLength'])
    sort_index = request.query['sortIndex']
    if sort_index == '-1':
        sort_index = 'time'
    search_term = request.query['searchTerm']

    sort_direction = r.desc if request.query['reverseSort']\
                                   .lower() == 'true' else r.asc

    print(page_start, page_start + page_length)

    # r.db('cion').table('tasks')
    # .orderBy({ index: "time" })
    # .filter(r.row("image-name").match("cion/api"))
    # .filter(r.row("event").match("new-image"))
    # .coerceTo('array').do(res => {
    #   return {result: res.slice(0, 20), length: res.count()};
    # })
    filter_func = search.get_filter(search_term)
    try:
        if not filter_func:
            result = await rdb_conn.conn.run(
                rdb_conn.conn.db().table('tasks')
                    .order_by(index=sort_direction(sort_index))
                    .slice(page_start, page_start + page_length)
                    .coerce_to('array')
            )

            # result = [row for row in result]
            # .slice(page_start,
            #        page_start + page_length)

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
                    # .slice(page_start,
                    #        page_start + page_length)
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

    # print(result)

    return web.Response(status=200,
                        text=json.dumps({'rows': result,
                                         'totalLength': count}),
                        content_type='application/json')
