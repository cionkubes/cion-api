import json

import luqum.parser
import rethinkdb as r
from aiohttp import web

import rdb_conn
import search


async def table_query(request, table_name):
    page_start = int(request.query['pageStart'])
    page_length = int(request.query['pageLength'])
    sort_index = request.query['sortIndex']
    if sort_index == '-1':
        sort_index = 'time'
    search_term = request.query['searchTerm']

    sort_direction = r.asc \
        if request.query['reverseSort'].lower() == 'true' \
        else r.desc

    try:
        filter_func = search.get_filter(search_term)
        if not filter_func:
            result = await rdb_conn.conn.run(
                rdb_conn.conn.db().table(table_name)
                    .order_by(index=sort_direction(sort_index))
                    .slice(page_start, page_start + page_length)
                    .coerce_to('array')
            )

            count = await rdb_conn.conn.run(
                rdb_conn.conn.db().table(table_name).count())
        else:
            try:
                db_res = await rdb_conn.conn.run(
                    rdb_conn.conn.db().table(table_name)
                        .order_by(index=sort_direction(sort_index))
                        .filter(filter_func)
                        .coerce_to('array')
                        .do(
                        lambda res: {
                            'result': res.slice(page_start,
                                                page_start + page_length),
                            'length': res.count()
                        })
                )

                result = db_res['result']
                count = db_res['length']
            except r.errors.ReqlResourceLimitError as e:
                return {
                    'web-response': web.Response(
                        status=400,
                        text=json.dumps({
                            'error': 'Unable to sort on given index. Too '
                                     'many results to sort, due to that the '
                                     'requested index not being an index in '
                                     'the database.'}),
                        content_type='application/json')
                }

    except luqum.parser.ParseError as e:
        return {
            'web-response': web.Response(
                status=400,
                text=json.dumps({
                    'error': 'Bad search term'}),
                content_type='application/json')
        }

    return {'result': result, 'count': count}
