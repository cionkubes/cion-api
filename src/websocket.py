import asyncio
from aiohttp import web
from aiohttp.web_ws import MsgType

from logzero import logger

app = web.Application()

socket_clients = []

async def websocket(request):
    ws = web.WebSocketResponse(autoclose=False)
    await ws.prepare(request)

    socket_clients.append(ws)

    while True:
        msg = await ws.receive()
        logger.debug(msg)
        if msg.type == MsgType.text:
            data = msg.json()

            ws.send_json(data)
            logger.info(f'Message')
            continue
        elif msg.type == MsgType.error:
            logger.debug('ws connection closed with exception %s' % ws.exception())

        break

    logger.info("Closing websocket.")
    socket_clients.remove(ws)
    ws.close()

    return ws

if __name__ == '__main__':
    import sys
    import os

    prod = len(sys.argv) > 1 and sys.argv[1].lower() == 'prod'

    routes = [
        # ('POST', '/api/v1/auth', api_auth),
        # ('POST', '/api/v1/usercreate', api_create_user),
        ('GET', '/api/ws', websocket)
    ]

    if not prod:
        static_path = os.path.join('..', '..', 'frontend', 'src', 'www')

        with open(os.path.join(static_path, 'spa-entry.html')) as f:
            indexfile = f.read()

        async def index(request):
            return web.Response(text=indexfile, content_type='text/html')

        app.router.add_static('/resources', os.path.join(static_path, 'resources'))
        routes.append(('GET', '/', index))

    for route in routes:
        app.router.add_route(*route)

    # sio.start_background_task(new_task_watch)
    web.run_app(app, host='0.0.0.0', port=5000)