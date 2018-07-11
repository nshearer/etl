from bunch import Bunch
from graphviz import Digraph
from threading import Lock
import re
import string

from jinja2 import Template

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
        self.__color_codes = dict()
        self.__next_color_code = 1


    def add_component(self, comp_id, name, inputs, outputs, state):
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
            state   = state))


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
            dot.node('comp_'+str(comp.comp_id), label=EtlGraphData.format_node_label(comp), shape='Mrecord')

        for conn in self.connections:
            dot.edge('%s:%s' % ('comp_'+str(conn.from_comp_id), 'o_' + sn(conn.from_port)),
                     '%s:%s' % ('comp_'+str(conn.to_comp_id), 'i_' + sn(conn.to_port)),)

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


    def get_gv_svg(self):
        '''
        Get SVG source from Graphviz

        Includes template variables for filling in dynamic data and colors.

        :return: SVG bytes
        '''
        cur_hash = self.structure_hash()
        with EtlGraphData.CACHE_LOCK:
            if EtlGraphData.TPL_CACHE_HASH is None or EtlGraphData.TPL_CACHE_HASH != cur_hash:
                EtlGraphData.TPL_CACHE = self._compile_gv_svg()
            return EtlGraphData.TPL_CACHE
