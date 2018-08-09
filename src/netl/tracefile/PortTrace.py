

from .TraceData import TraceData

class TracePort(TraceData):
    '''A port on a component in the ETL'''

    TABLE = 'component_ports'

    PORT_OPEN = 'open'
    PORT_CLOSED = 'closed'

    INPUT_PORT = 'i'
    OUTPUT_PORT = 'o'

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


    def _list_required_keys(self):
        return (
            'component_id', #  Unique, integer ID for the component this port is on
            'port_id',      #  Unique integer ID for this port
            'name',         #  Name of the port
            'port_type',    #  INPUT or OUTPUT
        )


class TracePortClosed(TraceData):
    '''Record that a port was closed'''

    def _list_required_keys(self):
        return tuple(
            'component_id', # Unique, integer ID for the component this port is on
            'port_id',      # Unique integer ID for this port
        )

