from http.server import HTTPServer
from socketserver import ThreadingMixIn
import logging

from netl_analyze import EtlAnalyzeHandler

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""


if __name__ == '__main__':
    server = ThreadedHTTPServer(('localhost', 8080), EtlAnalyzeHandler)
    print('Starting server, use <Ctrl-C> to stop')
    server.serve_forever()
