import os
from abc import ABC, abstractmethod

from jinja2 import Environment, select_autoescape
from jinja2 import BaseLoader, TemplateNotFound

from ..source import WebSource


class TemplateLoader(BaseLoader):
    '''Custom loader to retrieve templates from WebSource (html)'''

    SOURCE = WebSource()

    def get_source(self, environment, template):

        if not self.SOURCE.has(template):
            raise TemplateNotFound(template)

        source = self.SOURCE.get(template)
        source = source.decode('utf-8')

        source_path = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                   '..', 'html_content.py'))
        mtime = os.path.getmtime(source_path)

        return source, source_path, lambda: mtime == os.path.getmtime(source_path)


class ViewObject:
    '''Base class for dynamic html'''

    def __init__(self, db_pool, sources):
        self.db_pool = db_pool
        self.sources = sources


    def headers(self):
        return (
            ('Content-Type', 'text/html'),
        )


    @abstractmethod
    def render(self, url, matches):
        '''Render HTML content'''


    def render_template(self, tpl_name, **parms):
        env = Environment(
            loader = TemplateLoader(),
            autoescape=select_autoescape(['html', 'xml'])
        )
        tpl = env.get_template(tpl_name)
        return tpl.render(**parms)
