import asyncio
from aiohttp import web
from aiohttp.web_ws import MsgType

from logzero import logger
import rethinkdb as r


def create(conn):
    socket_clients = []

    async def listener():
        logger.info('starting task watch loop')

        cursor = await r.db('cion').table('tasks').changes().run(conn)

        while await cursor.fetch_next():
            change = await cursor.next()

            logger.debug(f"Change in tasks table: {change}")
            row = change['new_val']

            logger.debug(f"Dispatching row: {row}")

            msg = dict(event='task_update', data=row)
            for client in socket_clients:
                await client.send_json(msg)

            logger.debug('Row delivered')

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
            elif msg.type == MsgType.close:
                break

        logger.info("Closing websocket.")
        socket_clients.remove(ws)
        ws.close()

        return ws

    return listener(), websocket
