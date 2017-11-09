from aiohttp import web
from aiohttp.web_ws import MsgType
from collections import defaultdict

from logzero import logger


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
                    handler(self, ws, data['message'])
                elif msg.type == MsgType.error:
                    logger.debug('ws connection closed with exception %s' % ws.exception())
                elif msg.type == MsgType.close:
                    break
        finally:
            logger.info("Closing websocket.")
            self.clients.remove(ws)

            for sub in self.subscriptions[ws].values():
                sub.dispose()

            ws.close()

        return ws

    def subscribe(self, ws, message):
        subs = self.subscriptions[ws]
        table = message

        if table in subs:
            return

        def on_change(change):
            ws.send_json({"channel": f"changefeed-{table}", "message": change})

        subs[table] = self.conn.observe(message).subscribe(on_change)

    def unsubscribe(self, ws, message):
        subs = self.subscriptions[ws]
        table = message

        if table in subs:
            subs[table].dispose()

    dispatch = {
        "subscribe": subscribe,
        "unsubscribe": unsubscribe,
    }
