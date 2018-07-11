

from .TraceObject import TraceData, TraceAction

class RecordTrace(TraceData):
    '''A record in the ETL'''

    TABLE = 'records'

    CREATE_STATEMENTS = (
        """\
            create table records (
              id                  int primary key,
              origin_component_id int)
        """,
        """\
            create table record_attributes (
              rec_id              int,
              attr_name           text,
              value_id            int)
        """,
        """\
            create table record_data (
              id                  int primary key,
              value               text,
              CONSTRAINT unique_values UNIQUE (value))
        """,
    )


class TraceRecord(TraceAction):
    '''Save record to '''

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
                ConnectionTrace.CONN_OPEN))





