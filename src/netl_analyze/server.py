from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
import logging


from .database import DatabaseServicePool
from .


class EtlAnalyeHandler(BaseHTTPRequestHandler):

    DB_POOL = DatabaseServicePool(r'C:\Users\nshearer\Desktop\netl\src\test.trace')

    def do_GET(self):

        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.end_headers()
        # message = b"HTML"

        components = self.DB_POOL.request_multiple(lambda db: db.list_components())

        message = b"<html><body>"
        for component in components:
            message += b"<div>%s</div>" % (component.name.encode('utf-8'))
        message += b"</body></html>"

        self.wfile.write(message)
        self.wfile.write(b'\n')
        return

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""


if __name__ == '__main__':
    server = ThreadedHTTPServer(('localhost', 8080), EtlAnalyeHandler)
    print('Starting server, use <Ctrl-C> to stop')
    server.serve_forever()
