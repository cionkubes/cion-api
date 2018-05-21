import rdb_conn
from auth import requires_auth
from documents import json
from permissions.permission import perm
import rethinkdb as r

import table


async def db_create_environment(mode, name, tag_match, tls=None):
    data = {
        'mode': mode,
        'name': name,
        'tag-match': tag_match,
    }

    if tls:
        data['tls'] = tls

    return await rdb_conn.conn.run(
        r.db('cion').table('environments').insert(data))


@requires_auth(permission_expr=perm('cion.config.edit'))
async def create_environment(request):
    bod = await request.json()
    db_res = await db_create_environment(bod['mode'],
                                         bod['name'],
                                         bod['tag-match'],
                                         bod.get('tls', None))

    if 'errors' in db_res and db_res['errors']:
        if db_res['first_error'].find('Duplicate primary key') >= 0:
            text = f'Environment with name \'{bod["name"]}\' already exists'
        else:
            text = 'Something went wrong when inserting into database'
        return json({'error': text}, status=422)
    return json({'msg': 'Environment created'})


@requires_auth
async def get_environments(request):
    response = await table.table_query(request, 'environments')
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
