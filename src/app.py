from aiohttp import web
import rethinkdb as r
from logzero import logger
import socketio

sio = socketio.AsyncServer(async_mode='aiohttp')
app = web.Application()
sio.attach(app)


async def new_task_watch():
    logger.info('starting task watch loop')

    db_host = 'localhost'
    db_port = '28016'

    r.set_loop_type('asyncio')

    conn = await r.connect(db_host, db_port)
    cursor = await r.db('cion').table('tasks').changes().run(conn)

    while await cursor.fetch_next():
        change = await cursor.next()

        logger.debug(f"Change in tasks table: {change}")
        row = change['new_val']

        logger.debug(f"Dispatching row: {row}")
        await sio.emit(event='task_update', data=row, broadcast=True)
        logger.debug('Row delivered')


@sio.on('connect')
def user_connected(sid, environ):
    print('User connected')


@sio.on('disconnect')
def disconnect(sid):
    print('disconnect')


if __name__ == '__main__':
    import sys, os
    prod = len(sys.argv) > 1 and sys.argv[1].lower() == 'prod'

    if not prod:
        static_path = os.path.join('..', '..', 'frontend', 'src', 'www')

        with open(os.path.join(static_path, 'spa-entry.html')) as f:
            indexfile = f.read()

        async def index(request):
            return web.Response(text=indexfile, content_type='text/html')

        app.router.add_get('/', index)
        app.router.add_static('/resources', os.path.join(static_path, 'resources'))

    sio.start_background_task(new_task_watch)
    web.run_app(app, host='0.0.0.0', port=5000)
