from bunch import Bunch
from graphviz import Digraph
from threading import Lock
import re
from textwrap import dedent

from jinja2 import Template


def format_color_token(i):
    return '#%06d' % (i)

class EtlGraphData:
    '''
    Contains all the data needed to generate a graph of the ETL
    '''

    CACHE_LOCK = Lock()
    TPL_CACHE_HASH = None
    TPL_CACHE = None


    def __init__(self):
        self.components = list()
        self.connections = list()
        self.__next_color_token = 1


    def add_component(self, comp_id, name, inputs, outputs, state, state_color):
        '''
        Add a component

        :param comp_id: Unique ID of the comoponent
        :param name: Label for the component
        :param inputs: List of input port names
        :param outputs: List of output port names
        :param state: Current state of the component
        '''

        self.components.append(Bunch(
            comp_id = comp_id,
            name    = name,
            inputs  = inputs,
            outputs = outputs,
            state   = state,
            color_token = format_color_token(self.__next_color_token),
            color =   state_color,
            color_parm = "comp_%d_color" % (comp_id),
        ))

        self.__next_color_token += 1


    def add_connection(self, from_comp_id, from_port, to_comp_id, to_port):
        '''
        Add a connection (to be represented as a graph edge

        :param from_comp_id: ID of component sending data
        :param from_port: Name of output port
        :param to_comp_id: ID of component receiving data
        :param to_port: Name of input port
        '''
        self.connections.append(Bunch(
            from_comp_id   = from_comp_id,
            from_port      = from_port,
            to_comp_id     = to_comp_id,
            to_port        = to_port,
        ))


    @staticmethod
    def build_graph_data_for(trace_db):
        '''
        Generate a EtlGraphData to represent the data in a TraceDB

        :param trace_db: TraceDB with trace data from executed ETL
        '''
        graph = EtlGraphData()

        for comp in trace_db.list_components():
            graph.add_component(
                comp_id = comp.id,
                name = comp.name,
                inputs = [p.name for p in comp.list_input_ports()],
                outputs = [p.name for p in comp.list_output_ports()],
                state = comp.state_code,
                state_color = comp.state_color,
            )

        for conn in trace_db.list_connections():
            graph.add_connection(
                from_comp_id = conn.from_comp_id,
                from_port = conn.from_port_name,
                to_comp_id = conn.to_comp_id,
                to_port = conn.to_port_name)

        return graph


    def structure_hash(self):
        '''
        Generate a hash of the graph structure.

        If any data is set in this object that changes the structure of the graph,
        then the hash should change.  This is used primarilly to determine if we
        need to run graphviz again to regenerate the graph SVG.

        None-structural data is not included because it is passed to the template.
        '''
        hashstr = '|'.join((
            '|'.join(['|'.join((str(c.comp_id), c.name, '|'.join(c.inputs), '|'.join(c.outputs))) for c in self.components]),
            '|'.join(['|'.join((str(c.from_comp_id), c.from_port, str(c.to_comp_id), c.to_port)) for c in self.connections]),
        ))
        return hash(hashstr)


    UNSAFE_NAME_CHARS = re.compile(r'[^a-zA-Z0-9_-]')

    @staticmethod
    def sanitize_gv_name(name):
        return re.sub(EtlGraphData.UNSAFE_NAME_CHARS, '', name)


    def _compile_gv_svg(self):
        '''
        Have Graphviz genearate a SVG for this ETL graph

        :return: SVG bytes
        '''

        sn = EtlGraphData.sanitize_gv_name

        # Build graph
        dot = Digraph(comment='ETL', format='svg', graph_attr={'rankdir': "LR", })

        for comp in self.components:
            dot.node('comp_'+str(comp.comp_id),
                     label=EtlGraphData.format_node_label(comp),
                     shape='Mrecord',
                     color=comp.color_token)

        for conn in self.connections:
            dot.edge('%s:%s' % ('comp_'+str(conn.from_comp_id), 'o_' + sn(conn.from_port)),
                     '%s:%s' % ('comp_'+str(conn.to_comp_id), 'i_' + sn(conn.to_port)),
                     arrowhead = 'open', arrowsize = '0.5')

        # Call Graphviz
        return dot.pipe()


    @staticmethod
    def format_node_label(component):
        '''
        Form label of a component

        Nodes:
           +---------+
           |Component|
           +---------+
           |in1 |out1|
           +---------+
           |in2 |    |
           +----+----+

        struct1 [shape=Mrecord, label="<f0> Component | { { <in1> in1 | <in2> in2 } | { <out1> out1 |  } } "];

        :param component: Component data from this class
        :return: string to use as graphviz label for a Mrecord
        '''

        sn = EtlGraphData.sanitize_gv_name

        comp_label =  '%s' % (component.name)

        input_labels = ' | '.join(["<i_%s> %s" % (sn(name), name) for name in component.inputs])
        output_labels = ' | '.join(["<o_%s> %s" % (sn(name), name) for name in component.outputs])

        return "%s | { { %s } | { %s } }" % (comp_label, input_labels, output_labels)


    def _build_jinja_svg_template(self, svg):
        '''
        Create Jinja template SVG prepared to receive dynamic data

        :param src: SVG source from graphviz
        :return: Jinja Tempalate
        '''

        # Define replacements
        rep = dict()
        for component in self.components:
            rep[component.color_token] = "{{ %s }}" % (component.color_parm)

        # Perform replacements
        rep = dict((re.escape(k), v) for k, v in rep.items())
        pattern = re.compile("|".join(rep.keys()))

        svg_str = svg.decode('utf-8')
        svg_str = pattern.sub(lambda m: rep[re.escape(m.group(0))], svg_str)

        # Create template
        tpl = Template(svg_str)
        tpl.source = svg # Jinja2 doesn't give you the source back.  Storing it here
        return tpl


    def _render_jinja_svg(self, tpl):
        '''
        Render jinja SVG template to populate dynamic data

        :param tpl: Jinja Template from _build_jinja_svg_template()
        :return: SVG bytes
        '''
        parms = dict()

        for component in self.components:
            parms[component.color_parm] = component.color

        try:
            return tpl.render(**parms)
        except Exception as e:
            msg = dedent("""\
                Failed to render graph svg in _render_jinja_svg()
                
                Exception: {e}
                
                Template:
                {tpl}
                
                Parms:
                {parms}
                """).format(
                    e = str(e),
                    tpl = tpl.source,
                    parms = "\n".join(['%s = %s' % (k, repr(v)) for (k, v) in parms.items()])
                )
            raise Exception(msg)


    def get_svg(self):
        '''
        Get SVG source

        _compile_gv_svg() --> _build_jinja_svg_template() --> _render_jinja_svg()

        Includes template variables for filling in dynamic data and colors.

        :return: SVG bytes
        '''

        # Get SVG template with structure data
        cur_hash = self.structure_hash()
        with EtlGraphData.CACHE_LOCK:
            if EtlGraphData.TPL_CACHE_HASH is None or EtlGraphData.TPL_CACHE_HASH != cur_hash:
                svg = self._compile_gv_svg()
                svg = self._build_jinja_svg_template(svg)

                EtlGraphData.TPL_CACHE = svg
                EtlGraphData.TPL_CACHE_HASH = cur_hash

            tpl = EtlGraphData.TPL_CACHE

        # Run SVG template to fill in dynamic data
        svg = self._render_jinja_svg(tpl)
        return svg


