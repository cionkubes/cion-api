from aiohttp import web

import rdb_conn
import websocket
from services import get_service, delete_service, get_running_image, \
    get_services, create_service, edit_service
from documents import get_documents, set_document, get_document, \
    get_permission_def
from tasks import get_tasks, create_task, get_recent_tasks, get_task
from auth import api_auth, api_create_user, logout, verify_token
from cion_system import get_health
from user import set_gravatar_email, get_users, delete_user, change_password, \
    get_permissions, set_permissions, change_own_password

app = web.Application()

if __name__ == '__main__':
    import sys
    import os

    prod = len(sys.argv) > 1 and sys.argv[1].lower() == 'prod'

    if not prod:
        static_path = os.path.join(os.environ['WEB_DIR'], 'lib')

        async def index(request):
            with open(os.path.join(static_path, 'spa-entry.html')) as f:
                indexfile = f.read()
            return web.Response(text=indexfile, content_type='text/html')


        app.router.add_get('/', index)
        app.router.add_static('/resources',
                              os.path.join(static_path, 'resources'))

    rdb_conn.init()

    ws_route = websocket.create(rdb_conn.conn)

    app.router.add_get('/api/v1/socket', ws_route)

    app.router.add_post('/api/v1/auth', api_auth)
    app.router.add_get('/api/v1/verify-session', verify_token)
    app.router.add_post('/api/v1/create/user', api_create_user)
    app.router.add_post('/api/v1/logout', logout)

    app.router.add_post('/api/v1/user/setgravataremail', set_gravatar_email)
    app.router.add_put('/api/v1/user/setpassword', change_own_password)
    app.router.add_get('/api/v1/users', get_users)
    app.router.add_put('/api/v1/user/{name}/setpassword', change_password)
    app.router.add_delete('/api/v1/user/{name}', delete_user)

    app.router.add_get('/api/v1/tasks', get_tasks)
    app.router.add_get('/api/v1/tasks/recent', get_recent_tasks)
    app.router.add_get('/api/v1/task/{id}', get_task)
    app.router.add_post('/api/v1/create/task', create_task)

    app.router.add_get('/api/v1/documents', get_documents)
    app.router.add_post('/api/v1/documents', set_document)
    app.router.add_get('/api/v1/document/{name}', get_document)

    app.router.add_get('/api/v1/permissions/permission-def',
                       get_permission_def)
    app.router.add_get('/api/v1/permissions/user/{username}', get_permissions)
    app.router.add_put('/api/v1/permissions/user/{username}', set_permissions)

    app.router.add_get('/api/v1/services', get_services)
    app.router.add_post('/api/v1/services/create', create_service)
    app.router.add_get('/api/v1/service/image/{name}', get_running_image)
    app.router.add_get('/api/v1/service/{name}', get_service)
    app.router.add_put('/api/v1/service/{name}/edit', edit_service)
    app.router.add_delete('/api/v1/service/{name}', delete_service)

    app.router.add_get('/api/v1/health', get_health)

    web.run_app(app, host='0.0.0.0', port=5000)
