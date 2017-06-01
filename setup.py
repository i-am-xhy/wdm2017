import os
import falcon
import json
from api.code.apiserver import add_api_routes
from waitress import serve

WAITRESS_PORT = '5000'

if __name__ == '__main__':
    app = falcon.API()
    add_api_routes(app)
    if 'PORT' in os.environ:
        port = os.environ['PORT']
    else:
        port = WAITRESS_PORT
    serve(app, listen='*:' + port)
