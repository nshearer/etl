

from .TraceObject import TraceData, TraceAction

class PortTrace(TraceData):
    '''A port on a component in the ETL'''

    TABLE = 'component_ports'

    PORT_OPEN = 'open'
    PORT_CLOSED = 'closed'

    INPUT_PORT = 'i'
    OUTPUT_PORT = 'o'

    CREATE_STATEMENTS = (
        """\
            create table component_ports (
              comp_id     int,
              id          int primary key,
              name        text,
              port_type   text,
              state_code  text)
        """,
    )


class TraceComponentPortExists(TraceAction):
    '''Record the name of a port on a component'''

    def __init__(self, component_id, port_id, name, port_type):
        '''
        Tell the tracer about a component

        :param component_id: Unique, integer ID for the component this port is on
        :param port_id: Unique integer ID for this port
        :param name: Name of the port
        :param port_type: INPUT or OUTPUT
        '''
        super(TraceComponentPortExists, self).__init__()
        self.component_id = component_id
        self.port_id = port_id
        self.name = name
        self.state = PortTrace.PORT_OPEN
        self.port_type = port_type

    def record(self, trace_db):
        trace_db.execute_update("""
            insert into component_ports (comp_id, id, name, state_code, port_type)
            values (?, ?, ?, ?, ?)
            """, (
                int(self.component_id),
                self.port_id,
                self.name,
                self.state,
                self.port_type))


class TracePortClosed(TraceAction):
    '''Record that a port was closed'''

    def __init__(self, port_id):
        '''
        Tell the tracer about a component

        :param component_id: Unique, integer ID for the component this port is on
        :param port_id: Unique integer ID for this port
        '''
        super(TracePortClosed, self).__init__()
        self.port_id = port_id

    def record(self, trace_db):
        trace_db.execute_update("""
            update component_ports
            set state_code = ?
            where id = ?
            """, (PortTrace.PORT_CLOSED, self.port_id))

