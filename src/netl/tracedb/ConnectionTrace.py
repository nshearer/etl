

from .TraceObject import TraceData, TraceAction

class ConnectionTrace(TraceData):
    '''A connection between components in the ETL'''

    TABLE = 'component_ports'

    CONN_OPEN = 'open'
    CONN_CLOSED = 'closed'

    INPUT_PORT = 'i'
    OUTPUT_PORT = 'o'

    CREATE_STATEMENTS = (
        """\
            create table connections (
              output_port_id    int,
              input_port_id     int,
              state_code        text)
        """,
        """\
            create view v_connection_detail as
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
        """,
    )


    @staticmethod
    def list_connections(trace_db):
        for row in trace_db.execute_select("select * from connection_detail"):
            yield ConnectionTrace(trace_db, **row)


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

    def record_trace_to_db(self, trace_db, commit):
        trace_db.execute_update("""
            insert into connections (output_port_id, input_port_id, state_code)
            values (?, ?, ?)
            """, (
                int(self.from_port_id),
                int(self.to_port_id),
                ConnectionTrace.CONN_OPEN),
            commit=commit)


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

    def record_trace_to_db(self, trace_db, commit):
        trace_db.execute_update("""
            update connections
            set state_code = ?
            where output_port_id = ?
              and input_port_id = ?
            """, (
                ConnectionTrace.CONN_CLOSED,
                int(self.from_port_id),
                int(self.to_port_id),
                ),
            commit = commit)
