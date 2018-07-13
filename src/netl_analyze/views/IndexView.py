import re

from .ViewObject import ViewObject

class IndexView(ViewObject):


    PAT=re.compile(r'^(index\.html)?$')


    def render(self, url, matches):

        return self.render_template(
            tpl_name = 'index.j2.html',
            status = self.trace_db.etl_status(),
            components = self.trace_db.list_components(),
        )
