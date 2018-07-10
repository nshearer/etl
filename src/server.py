import os, sys
from http.server import HTTPServer
from socketserver import ThreadingMixIn
import logging

from netl_analyze import EtlAnalyzeHandler

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""


if __name__ == '__main__':
    try:
        EtlAnalyzeHandler.set_trace_path(sys.argv[1])
    except:
        print("Usage: %s trace_path" % (os.path.basename(sys.argv[0])))
        sys.exit(1)

    server = ThreadedHTTPServer(('localhost', 8080), EtlAnalyzeHandler)
    print('Starting server, use <Ctrl-C> to stop')
    server.serve_forever()
