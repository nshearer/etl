

from .TraceObject import TraceData, TraceAction

class PortTrace(TraceData):
    '''A port on a component in the ETL'''

    TABLE = 'component_ports'

    PORT_OPEN = 'open'
    PORT_CLOSED = 'closed'

    CONN_OPEN = 'open'
    CONN_CLOSED = 'closed'

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
        """\
            create table connections (
              output_port_id    int,
              input_port_id     int,
              state_code        text)
        """,
        """\
            create view connection_detail as
            select
              fc.id           as from_comp_id,
              fc.name         as from_comp_name,
              fc.state_code   as from_comp_state,
              fp.id           as from_port_id,
              fp.name         as from_port_name,
              fp.state_code   as from_port_state,
              c.state_code    as conn_state,
              tc.id           as to_comp_id,
              tc.name         as to_comp_name,
              tc.state_code   as to_comp_state,
              tp.id           as to_port_id,
              tp.name         as to_port_name,
              tp.state_code   as to_port_state
            from connections c
            left join component_ports fp on fp.id = c.output_port_id
            left join components fc on fc.id = fp.comp_id
            left join component_ports tp on tp.id = c.input_port_id
            left join components tc on tc.id = tp.comp_id
        """
    )


    @staticmethod
    def list_ports_for(trace_db, component_id, port_type=None):
        results = trace_db.execute_select("""\
          select *
          from component_ports
          where comp_id = :comp_id
            and (:port_type is null or port_type = :port_type)
          """, {
            'comp_id': component_id,
            'port_type': port_type,
        })
        for row in results:
            yield PortTrace(trace_db, row)





class TraceComponentPortExists(TraceAction):
    '''Record the name of a port on a component'''

    def __init__(self, component_id, port_id, name, port_type):
        '''
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


class TraceConnection(TraceAction):
    '''Record that the output of one component was connected to an input on another'''

    def __init__(self, from_port_id, to_port_id):
        '''
        :param from_port_id: Unique integer ID of the output port being
        :param to_port_id: Unique integer ID for the input port
        '''
        super(TraceConnection, self).__init__()
        self.from_port_id = from_port_id
        self.to_port_id = to_port_id

    def record(self, trace_db):
        trace_db.execute_update("""
            insert into connections (output_port_id, input_port_id, state_code)
            values (?, ?, ?)
            """, (
                int(self.from_port_id),
                int(self.to_port_id),
                PortTrace.CONN_OPEN))


class TraceConnectionClosed(TraceAction):
    '''Record that the output of one component was connected to an input on another'''

    def __init__(self, from_port_id, to_port_id):
        '''
        :param from_port_id: Unique integer ID of the output port being
        :param to_port_id: Unique integer ID for the input port
        '''
        super(TraceConnectionClosed, self).__init__()
        self.from_port_id = from_port_id
        self.to_port_id = to_port_id

    def record(self, trace_db):
        trace_db.execute_update("""
            update connections
            set state_code = ?
            where output_port_id = ?
              and input_port_id = ?
            """, (
                PortTrace.CONN_CLOSED,
                int(self.from_port_id),
                int(self.to_port_id),
                ))
