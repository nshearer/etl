
from ..utils import RWLock

from ..RecordShelf import RecordShelf
from .etl_workflow import EtlState

class TraceData:
    '''
    Collected trace data to be queried
    '''

    def __init__(self):
        self.lock = RWLock()

        self.records = RecordShelf()

        self.etl_state = EtlState()
        self.components = dict()        # [component_id] = ComponentState()
        self.ports = dict()             # [port_id] = PortState()


    # -- Queries -----------------------------------------------------------------------

    # TODO: etl_status(self):
    #     return self.execute_select_one("select state_code from etl")['state_code']
    #
    # TODO: list_components(self):
    #     return ComponentTrace.list_components(self)
    #
    # TODO: list_ports_for(self, component_id, port_type=None):
    #     return PortTrace.list_ports_for(self, component_id, port_type)
    #
    # TODO: list_connections(self):
    #     return ConnectionTrace.list_connections(self)
    #
    # TODO: get_connection_stats(self):
    #     return EnvelopeTrace.get_connection_stats(self)





    # TODO: list_components(trace_db):
    #     results = trace_db.execute_select("select * from components")
    #     for row in results:
    #         yield ComponentTrace(trace_db, **row)
    #
    #
    # TODO: list_ports(self):
    #     return self.db.list_ports_for(self.id)
    #
    # TODO: list_input_ports(self):
    #     return self.db.list_ports_for(self.id, port_type='i')
    #
    # TODO: list_output_ports(self):
    #     return self.db.list_ports_for(self.id, port_type='o')


    # @staticmethod
    # TODO:  list_connections(trace_db):
    #     for row in trace_db.execute_select("select * from v_connection_detail"):
    #         yield ConnectionTrace(trace_db, **row)


    # @staticmethod
    # TODO: list_ports_for(trace_db, component_id, port_type=None):
    #     results = trace_db.execute_select("""\
    #       select *
    #       from component_ports
    #       where comp_id = :comp_id
    #         and (:port_type is null or port_type = :port_type)
    #       """, {
    #         'comp_id': component_id,
    #         'port_type': port_type,
    #     })
    #     for row in results:
    #         yield PortTrace(trace_db, **row)



