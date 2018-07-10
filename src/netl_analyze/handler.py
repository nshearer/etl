from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
from mimetypes import guess_type
from textwrap import dedent
import logging

from .database import DatabaseServicePool
from .source import WebSource
from . import views

class EtlAnalyzeHandler(BaseHTTPRequestHandler):

    DB_POOL = None
    SOURCE = WebSource()

    @staticmethod
    def set_trace_path(path):
        EtlAnalyzeHandler.DB_POOL = DatabaseServicePool(path)


    def _get_view_classes(self):
        for cls_name in filter(lambda c: c.endswith('View'), dir(views)):
            yield getattr(views, cls_name)


    def do_GET(self):

        # Parse URL
        url = urlparse(self.path)
        path = url.path.lstrip('/')

        # Return static content
        if self.SOURCE.has(path):
            self.send_response(200)

            mimetype = guess_type(path)
            if mimetype[0] is None:
                print("ERROR: Can't determine mimetype for " + path)
            else:
                self.send_header('Content-Type', mimetype[0])
            self.end_headers()

            contents = self.SOURCE.get(path)
            self.wfile.write(contents)


        # Check dynamic views
        for cls in self._get_view_classes():
            m = cls.PAT.match(path)
            if m:
                view = cls(self.DB_POOL, self.SOURCE)
                try:
                    html = view.render(url = url, matches = m)

                except Exception as e:
                    self.send_response(500)
                    self.send_header('Content-Type', 'text/html')
                    self.end_headers()

                    self.wfile.write(dedent("""\
                        <html>
                        <head><title>Error</title></head>
                        <body>
                        <h1>Error: {ecls}</h1>
                        <pre>{error}</pre>
                    """).format(
                        ecls = e.__class__.__name__,
                        error = str(e)
                    ).encode('utf-8'))

                    return

                self.send_response(200)

                for header, value in view.headers():
                    self.send_header(header, value)
                self.end_headers()

                self.wfile.write(html.encode('utf-8'))


