import re

from .ViewObject import ViewObject

class IndexView(ViewObject):


    PAT=re.compile(r'^(index\.html)?$')


    def render(self, url, matches):

        return self.render_template(
            tpl_name = 'index.j2.html'
        )
