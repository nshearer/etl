

from .TraceObject import TraceObject

class ComponentTrace(TraceObject):
    '''A component in the ETL'''

    TABLE = 'components'

    CREATE_STATEMENTS = (
        """\
            create table components (
              id          int primary key,
              name        text,
              class       text,
              state_code  text)
        """,
    )

    def __init__(self, db, ):
        self.trace_db = db

    @staticmethod
    def trace_new_component(trace_db, component_id, name, clsname, state):
        '''Record the existance of a component in the ETL'''
        trace_db.assert_readwrite()

        trace_db.sqlite3db.cursor().execute("""
            insert into components (id, name, class, state_code)
            values (?, ?, ?, ?)
            """, (int(component_id), name, clsname, state))
        trace_db.sqlite3db.commit()

    @staticmethod
    def trace_state_change(trace_db, component_id, state_code):
        '''Record the change of state on a component'''
        trace_db.assert_readwrite()

        trace_db.sqlite3db.cursor().execute("""
            update components
            set state_code = ?
            where id = ?
            """, (state_code, int(component_id)))
        trace_db.sqlite3db.commit()