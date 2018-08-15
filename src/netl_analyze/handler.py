import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
from mimetypes import guess_type
from textwrap import dedent
import traceback

from netl.tracefile import TraceFileMonitor

from .source import WebSource
from . import views

class EtlAnalyzeHandler(BaseHTTPRequestHandler):

    TRACE_READER = None
    SOURCE = WebSource()

    @staticmethod
    def set_trace_path(path):
        EtlAnalyzeHandler.TRACE_READER = TraceFileMonitor(path)
        EtlAnalyzeHandler.TRACE_READER.start()


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
                view = cls(self.DB, self.SOURCE)
                try:
                    html = view.render(url = url, matches = m)

                except Exception as e:
                    exc_type, exc_value, exc_traceback = sys.exc_info()

                    self.send_response(500)
                    self.send_header('Content-Type', 'text/html')
                    self.end_headers()

                    self.wfile.write(dedent("""\
                        <html>
                        <head><title>Error</title></head>
                        <body>
                        <h1>Error: {ecls}</h1>
                        <div>{tb}</div>
                        </body></html>
                    """).format(
                        ecls = e.__class__.__name__,
                        tb = "\n".join(["<div><pre>%s</prd></div>" % (l) for l in traceback.format_exception(
                            exc_type, exc_value, exc_traceback)])
                    ).encode('utf-8'))

                    return

                self.send_response(200)

                for header, value in view.headers():
                    self.send_header(header, value)
                self.end_headers()

                self.wfile.write(view.encode_output(html))


