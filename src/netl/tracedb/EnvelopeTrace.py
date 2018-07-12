

from .TraceObject import TraceData, TraceAction

class EnvelopeTrace(TraceData):
    '''A transmission trace for a record in the ETL'''

    TABLE = 'record_dispatch'

    CREATE_STATEMENTS = (
        """\
            create table record_dispatch (
              record_id         int,
              from_port_id      int,
              to_port_id        int)
        """,
    )


class TraceRecordDispatch(TraceAction):
    '''Record that a record was sent out'''

    def __init__(self, record_id, from_port_id, to_port_id):
        '''
        :param envilope: EtlRecordEnvelope with message detail
        '''
        super(TraceRecordDispatch, self).__init__()
        self.record_id  = record_id
        self.from_port_id = from_port_id
        self.to_port_id = to_port_id


    def record_trace_to_db(self, trace_db, commit):
        trace_db.execute_update("""
            insert into record_dispatch (record_id, from_port_id, to_port_id)
            values (?, ?, ?)
            """, (
                int(self.record_id),
                int(self.from_port_id),
                int(self.to_port_id)),
            commit=commit)
