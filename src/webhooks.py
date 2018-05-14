import rdb_conn
from auth import requires_auth
from documents import json
from permissions.permission import perm
import rethinkdb as r

import table


async def db_create_webhook(url, event, match_on, headers, data):
    data = {
        'url': url,
        'event': event,
        'on': match_on,
        'headers': headers,
        'data': data
    }

    return await rdb_conn.conn.run(
        r.db('cion').table('webhooks').insert(data))


@requires_auth(permission_expr=perm('cion.config.edit'))
async def create_webhook(request):
    bod = await request.json()
    db_res = await db_create_webhook(bod['url'],
                                     bod['event'],
                                     bod['on'],
                                     bod['headers'],
                                     bod['data'])

    if 'errors' in db_res and db_res['errors']:
        if db_res['first_error'].find('Duplicate primary key') >= 0:
            text = f'Webhook with name \'{bod["name"]}\' already exists'
        else:
            text = 'Something went wrong when inserting into database'
        return json({'error': text}, status=422)
    return json({'msg': 'Webhook created'})


@requires_auth
async def get_webhooks(request):
    response = await table.table_query(request, 'webhooks')
    if 'web-response' in response:
        return response['web-response']
    else:
        result = response['result']
        count = response['count']

    return json({'rows': result, 'totalLength': count})


@requires_auth
async def get_webhook(request):
    webhook_id = request.match_info['id']

    result = await rdb_conn.conn.run(
        rdb_conn.conn.db().table('webhooks').get(webhook_id))

    return json(result)


@requires_auth(permission_expr=perm('cion.config.edit'))
async def delete_webhook(request):
    webhook_id = request.match_info['id']

    result = await rdb_conn.conn.run(
        rdb_conn.conn.db().table('webhooks').get(webhook_id).delete())

    return json(result)
