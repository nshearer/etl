import os
import sqlite3

from .ComponentTrace import ComponentTrace
from .PortTrace import PortTrace
from .EnvelopeTrace import EnvelopeTrace
from .RecordTrace import RecordTrace


class TraceDB:
    '''Record of ETL progress'''

    VERSION='dev'

    CREATE_STATEMENTS = (
        """\
        create table etl(
          state_code  text,
          db_ver      text)
        """,
    )


    INIT_STATE = 'init'
    RUNNING_STATE = 'running'
    FINISHED_STATE = 'finshed'
    ERROR_SATE = 'error'


    def __init__(self, path, mode='r'):
        self.__db = sqlite3.connect(path)

        if mode == 'r':
            self.__readonly = True
            if not os.path.exists(path):
                raise Exception("TraceDB %s doesn't exist" % (path))
        elif mode == 'rw':
            self.__readonly = False
        else:
            raise Exception("mode must be r or rw")


    @property
    def sqlite3db(self):
        if self.__db is None:
            raise Exception("Database closed")
        return self.__db


    @property
    def readonly(self):
        return self.__readonly


    def assert_readwrite(self):
        if self.__readonly:
            raise Exception("Database open for read only")


    def close(self):
        self.__db.close()
        self.__db = None


    @staticmethod
    def create(path, mode='rw'):

        if os.path.exists(path):
            raise Exception("Trace DB file already exists: " + path)
        db = sqlite3.connect(path)

        create_statments = list(TraceDB.CREATE_STATEMENTS)
        create_statments.extend(ComponentTrace.CREATE_STATEMENTS)
        create_statments.extend(PortTrace.CREATE_STATEMENTS)
        create_statments.extend(EnvelopeTrace.CREATE_STATEMENTS)
        create_statments.extend(RecordTrace.CREATE_STATEMENTS)

        for sql in create_statments:
            db.cursor().execute(sql)

        db.close()
        return TraceDB(path)



    def list_components(self):
        return ComponentTrace.list_components(self)



    # == Tracing methods ==============================================

    def trace_new_component(self, **attrs):
        attrs['trace_db'] = self
        ComponentTrace.trace_new_component(**attrs)

    def trace_component_state_change(self, **attrs):
        attrs['trace_db'] = self
        ComponentTrace.trace_state_change(**attrs)



# TODO: Extra trace objects

                # # Record Derivation Trace Table: Trace when one record value is referenced to calucate the value of another
                # db.cursor().execute("""
                #     create table record_derivation (
                #       ref_record_id     int,
                #       ref_record_attr   text,
                #       calc_record_id    int,
                #       calc_record_attr  text)
                # """)
                #
                # # Component Status Table: Large record storage
                # db.cursor().execute("""
                #     create table large_records (
                #       id                int primary key)
                # """)
                # db.cursor().execute("""
                #     create table large_record_data (
                #       large_rec_id      int,
                #       chunk_num         int,
                #       data              text)
                # """)
