import re

from .ViewObject import ViewObject
from ..graphs import EtlGraphData

class EtlGraphView(ViewObject):


    PAT=re.compile(r'^graph\.svg$')
    assert(PAT.match('graph.svg'))

    def headers(self):
        return (
            ('Content-Type', 'image/svg+xml'),
        )


    def render(self, url, matches):

        graph = EtlGraphData.build_graph_data_for(self.trace_db)
        svg = graph.get_gv_svg()
        return svg


    def encode_output(self, output):
        # Output is already in bytes
        return output