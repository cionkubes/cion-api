from aiohttp import web
from aiohttp.web_ws import MsgType
from collections import defaultdict

from logzero import logger

from aioreactive.core import subscribe, AsyncAnonymousObserver


def create(conn):
    socket = WebSocketListener(conn)
    return socket.handle_request


class WebSocketListener:
    def __init__(self, conn):
        self.clients = []
        self.subscriptions = defaultdict(dict)
        self.conn = conn

    async def handle_request(self, request):
        ws = web.WebSocketResponse(autoclose=False)
        await ws.prepare(request)

        self.clients.append(ws)

        try:
            while True:
                msg = await ws.receive()
                logger.debug(msg)
                if msg.type == MsgType.text:
                    data = msg.json()

                    handler = WebSocketListener.dispatch[data['channel']]
                    await handler(self, ws, data['message'])
                elif msg.type == MsgType.error:
                    logger.debug('ws connection closed with exception %s' % ws.exception())
                elif msg.type == MsgType.close:
                    break
        except Exception as e:
            logger.exception("Unhandled exception in socket request handler.")
        finally:
            logger.info("Closing websocket.")
            self.clients.remove(ws)

            for sub in self.subscriptions[ws].values():
                await sub.adispose()

            ws.close()

        return ws

    async def subscribe(self, ws, message):
        subs = self.subscriptions[ws]
        table = message

        if table in subs:
            logger.debug("Client attempting to subscribe to already subscribed table")

        async def on_change(change):
            ws.send_json({"channel": f"changefeed-{table}", "type": "next", "message": change})

        async def on_error(error):
            logger.warn(error)
            ws.send_json({"channel": f"changefeed-{table}", "type": "error", "message": str(error)})

        async def on_complete():
            del subs[table]

        subs[table] = await subscribe(
            self.conn.observe(message), 
            AsyncAnonymousObserver(on_change, on_error, on_complete)
        )

    async def unsubscribe(self, ws, message):
        subs = self.subscriptions[ws]
        table = message

        if table in subs:
            await subs[table].adispose()
            del subs[table]

    dispatch = {
        "subscribe": subscribe,
        "unsubscribe": unsubscribe,
    }
