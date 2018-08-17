

from .TraceEvent import TraceEvent


class PortState:
    '''Tracking of port state in TraceData'''

    PORT_OPEN = 'open'
    PORT_CLOSED = 'closed'

    INPUT_PORT = 'i'
    OUTPUT_PORT = 'o'

    def __init__(self, event):
        self.component_id = event.component_id
        self.port_id = event.port_id
        self.name = event.name
        self.port_type = event.port_type

        self.port_state = self.PORT_OPEN

        self.connections = dict()      # [to_port_id] = list(Outbound|InboundConnectionState(), )


class TracePort(TraceEvent):
    '''A port on a component in the ETL'''

    def _list_required_keys(self):
        return (
            'component_id', #  Unique, integer ID for the component this port is on
            'port_id',      #  Unique integer ID for this port
            'name',         #  Name of the port
            'port_type',    #  INPUT or OUTPUT
        )

    def apply_to_trace_data(self, trace_data):
        state = PortState(self)
        trace_data.ports[self.port_id] = state
        trace_data.components[self.component_id].ports[self.port_id] = state


class TracePortClosed(TraceEvent):
    '''Record that a port was closed'''

    def _list_required_keys(self):
        return tuple((
            'component_id', # Unique, integer ID for the component this port is on
            'port_id',      # Unique integer ID for this port
        ))

    def apply_to_trace_data(self, trace_data):
        trace_data.ports[self.port_id].port_state = PortState.PORT_CLOSED
