import json

from aiohttp import web


async def get_health(request):
    """
    aiohttp endpoint to act as a healthcheck

    :param request: aiohttp request object
    :return: an aiohttp response object with http status code **200**.
    """
    return web.Response(status=200,
                        text=json.dumps({'status': 'UP'}),
                        content_type='application/json')
