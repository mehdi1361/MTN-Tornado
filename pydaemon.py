#!/usr/bin/env python2.7
import logging
from tornado import web, ioloop
from DatabaseOp import update_subscribe
import datetime
import daemon
LOG_FILENAME = 'Log/RequestLog.out'
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG,)


class IndexHandler(web.RequestHandler):
    ''' index http normal handler'''
    def get(self):
        # self.render("index.html")
        self.write("service worked")


class ApiHandler(web.RequestHandler):
    @web.asynchronous
    def get(self, *args):

        self._data = {
            'subscriber': self.get_argument("subscriber"),
            'service_id': self.get_argument("service_id"),
            'subscribed': self.get_argument("subscribed"),
            'status': self.get_argument("status"),
            'shortcode': self.get_argument("shortcode"),
        }
        logging.debug('This message should go to the log file')
        logging.info(self.request.remote_ip)
        logging.info(self.request.remote_ip)
        logging.info(" [%s] " % datetime.datetime.now())
        logging.info(self.request.uri)
        self.write("true")
        self.finish()

    def on_finish(self):
            update_subscribe.delay(**self._data)

app = web.Application([
    (r'/', IndexHandler),
    (r'/api', ApiHandler),
])

def runserver():
    # log = open('tornado.' + str(2053) + '.log', 'a+')
    # ctx = daemon.DaemonContext(stdout=log, stderr=log,  working_directory='.')
    # ctx.open()
    # logging.debug('ruserver started!!!!')
    app.listen(2053)
    ioloop.IOLoop.instance().start()

if __name__ == '__main__':
    runserver()
