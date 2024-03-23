
import time

import tornado.web

from utilities import logger
from utilities.status_code import *


class HelloHandler(tornado.web.RequestHandler):

    def prepare(self):
        self.set_header('Content-Type', 'application/json;')
        self.set_header("Server", "MPC-Comp/Version-1.0")

    def get(self):
        logger.debug("Default Handler Called.")
        self.write({
            'code': 0,
            'msg': 'Hello World from Flow Controller!',
            'data': {
                'current_time': time.ctime(),
                }
            })
    post = get 
        

class DefaultHandler(tornado.web.RequestHandler):
    def prepare(self):
        self.set_header('Content-Type', 'application/json;')
        self.set_header("Server", "MPC-Comp/Version-1.0")

    def post(self, kw):
        logger.warning('Wrong Path is Requested (POST): '  +'/' + kw)
        self.set_status(404, "NOT Found")
        self.write({
            'code': 404,
            'msg': '404 NOT Found',
            'data': {}
            })

    def get(self, kw):
        logger.warning('Wrong Path is Requested (GET): ' +'/' +kw)
        self.set_status(METHOD_NOT_ALLOWED, status_msg_dict[METHOD_NOT_ALLOWED])
        self.write({
            'code': METHOD_NOT_ALLOWED,
            'msg': status_msg_dict[METHOD_NOT_ALLOWED],
            'data': {}
            })