
from .TraceData import TraceData

class TraceConnection(TraceData):
    '''Record that the output of one component was connected to an input on another'''

    CONN_OPEN = 'open'
    CONN_CLOSED = 'closed'

    INPUT_PORT = 'i'
    OUTPUT_PORT = 'o'

    # @staticmethod
    # TODO:  list_connections(trace_db):
    #     for row in trace_db.execute_select("select * from v_connection_detail"):
    #         yield ConnectionTrace(trace_db, **row)

    def _list_required_keys(self):
        return (
            'from_port_id', # Unique integer ID of the output port being
            'to_port_id',   # Unique integer ID for the input port
        )


class TraceConnectionClosed(TraceData):
    '''Record that a connection has been closed'''

    def _list_required_keys(self):
        return tuple(
            'from_port_id', # Unique integer ID of the output port being
            'to_port_id',   # Unique integer ID for the input port
        )

