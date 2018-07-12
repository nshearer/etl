from bunch import Bunch

from .TraceObject import TraceData, TraceAction


class ConnectionStats(Bunch): pass


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
        """\
            create view v_connection_stats as
            select
              c.from_comp_id,
              c.from_comp_name,
              c.from_port_id,
              c.from_port_name,
              c.to_comp_id,
              c.to_comp_name,
              c.to_port_id,
              c.to_port_name,
              count(*)    rec_count
            from v_connection_detail c
            left join record_dispatch d on d.from_port_id = c.from_port_id
                                       and d.to_port_id = c.to_port_id
            group by
              c.from_comp_id,
              c.from_comp_name,
              c.from_port_id,
              c.from_port_name,
              c.to_comp_id,
              c.to_comp_name,
              c.to_port_id,
              c.to_port_name
        """
    )


    @staticmethod
    def get_connection_stats(trace_db):
        '''Get some stats on messages sent on connections'''
        for row in trace_db.execute_select("select * from v_connection_stats"):
            yield ConnectionStats(**row)



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
