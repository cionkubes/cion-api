import json

from aiohttp import web


async def get_health(request):
    return web.Response(status=200,
                        text=json.dumps({'status': 'UP'}),
                        content_type='application/json')
