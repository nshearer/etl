import re

from .ViewObject import ViewObject

class StatusCardView(ViewObject):


    PAT=re.compile(r'^status\.html$')


    def render(self, url, matches):

        return self.render_template(
            tpl_name = 'status-card.j2.html',
            status = self.trace_db.etl_status(),
        )
