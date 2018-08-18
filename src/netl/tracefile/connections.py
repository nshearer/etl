
from .TraceEvent import TraceEvent


class OutboundConnectionState:
    '''Stores the current state of a connection sending data to another ccmponent'''

    CONN_OPEN = 'open'
    CONN_CLOSED = 'closed'

    def __init__(self, port_id, remote_port_id):
        self.port_id = port_id
        self.remote_port_id = remote_port_id
        self.state = self.CONN_OPEN


class InboundConnectionState(OutboundConnectionState):
    '''Stores the current state of a connection receiving data from another component'''


class TraceConnection(TraceEvent):
    '''Record that the output of one component was connected to an input on another'''

    def _list_required_keys(self):
        return (
            'from_port_id', # Unique integer ID of the output port being
            'to_port_id',   # Unique integer ID for the input port
        )

    def apply_to_trace_data(self, trace_data):
        trace_data.ports[self.from_port_id].connections[self.to_port_id] = OutboundConnectionState(
            port_id = self.from_port_id,
            remote_port_id = self.to_port_id)
        trace_data.ports[self.to_port_id].connections[self.from_port_id] = InboundConnectionState(
            port_id = self.to_port_id,
            remote_port_id = self.from_port_id)


class TraceConnectionClosed(TraceEvent):
    '''Record that a connection has been closed'''

    def _list_required_keys(self):
        return (
            'from_port_id', # Unique integer ID of the output port being
            'to_port_id',   # Unique integer ID for the input port
        )

    def apply_to_trace_data(self, trace_data):
        trace_data[self.from_port_id].connection[self.to_port_id].state = OutboundConnectionState.CONN_CLOSED
        trace_data[self.to_port_id].connection[self.from_port_id].state = InboundConnectionState.CONN_CLOSED

