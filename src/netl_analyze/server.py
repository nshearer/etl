import os, sys
from http.server import HTTPServer
from socketserver import ThreadingMixIn
from threading import Thread

from .handler import EtlAnalyzeHandler


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""


class NetAnalyzeHtmlServer:
    '''A threaded web server used to monitor and debug ETLs'''

    def __init__(self, trace_path, port=8080):
        self.__port = port
        self.__path = trace_path
        self.__http_server = ThreadedHTTPServer(('localhost', 8080), EtlAnalyzeHandler)
        self.__server_thread = None

        EtlAnalyzeHandler.set_trace_path(trace_path)


    @property
    def port(self):
        return self.__port


    @property
    def path(self):
        return self.__port


    def server_forever(self):
        '''Begin serving clients, doesn't exit'''
        return self.__http_server.serve_forever()


    def start_thread(self):
        '''Start HTML server in a separate thread and return'''
        self.__server_thread = Thread(target=lambda: self.server_forever)
        self.__server_thread.daemon = True # Exit when parent exists
        self.__server_thread.start()


