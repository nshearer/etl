from bunch import Bunch
from graphviz import Digraph
from threading import Lock


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
        data = EtlGraphData()

        for comp in trace_db.list_components():
            data.add_component(
                comp_id = comp.id,
                name = comp.name,
                inputs = [p.name for p in comp.list_input_ports()],
                outputs = [p.name for p in comp.list_output_ports()],
                state = comp.state_code,
            )


    def structure_hash(self):
        '''
        Generate a hash of the graph structure.

        If any data is set in this object that changes the structure of the graph,
        then the hash should change.  This is used primarilly to determine if we
        need to run graphviz again to regenerate the graph SVG.

        None-structural data is not included because it is passed to the template.
        '''
        hashstr = '|'.join((
            '|'.join([(str(c.comp_id), c.name, '|'.join(c.inputs), '|'.join(c.outputs)) for c in self.components]),
            '|'.join([(str(c.from_comp_id), c.from_port, str(c.to_comp_id), c.to_port) for c in self.connections]),
        ))
        return hash(hashstr)


    def _compile_gv_svg(self):
        '''
        Have Graphviz genearate a SVG for this ETL graph

        :return: SVG bytes
        '''

        # Build graph
        dot = Digraph(comment='ETL', format='svg')

        for comp in self.components:
            dot.node('comp_'+str(comp.comp_id), comp.name)

        # Call Graphviz
        return dot.pipe()

