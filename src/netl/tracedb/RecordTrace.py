

from .TraceObject import TraceObject

class RecordTrace(TraceObject):
    '''A record in the ETL'''

    TABLE = 'records'

    CREATE_STATEMENTS = (
        """\
            create table records (
              id                int primary key,
              origin_component  int,
              large_record      int,
              data              text)
        """,
    )


