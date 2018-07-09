

from .TraceObject import TraceData

class PortTrace(TraceData):
    '''A port on a component in the ETL'''

    TABLE = 'component_ports'

    CREATE_STATEMENTS = (
        """\
            create table component_ports (
              comp_id     int,
              id          int primary key,
              name        text,
              state_code  text)
        """,
    )


