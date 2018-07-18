
from .TraceData import TraceData

class TraceEtlState(TraceData):

    INIT_STATE = 'init'
    RUNNING_STATE = 'running'
    FINISHED_STATE = 'finished'
    ERROR_STATE = 'error'

    def _list_required_keys(self):
        return (
            'state',    # State code of the ETL
        )



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




