

from .TraceObject import TraceData

class EnvelopeTrace(TraceData):
    '''A transmission trace for a record in the ETL'''

    TABLE = 'envelopes'

    CREATE_STATEMENTS = (
        """\
            create table envelopes (
              id                int primary key,
              record_id         int,
              from_port_id      int,
              to_port_id        int)
        """,
    )

