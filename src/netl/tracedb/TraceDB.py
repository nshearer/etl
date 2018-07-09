import os
import sqlite3
from threading import Lock

from .ComponentTrace import ComponentTrace
from .PortTrace import PortTrace
from .EnvelopeTrace import EnvelopeTrace
from .RecordTrace import RecordTrace

from ..utils import ResultBuffer


class TraceDB:
    '''
    Interface to the ETL tracing database tracking ETL progress

    This object serves as a single handle to the database connection
    to use is a multi-threaded environment for both updating the database
    with new events and reading the trace data.

    According to the manual for sqlite3:

        By default, check_same_thread is True and only the creating
        thread may use the connection. If set False, the returned
        connection may be shared across multiple threads. When
        using multiple threads with the same connection writing
        operations should be serialized by the user to avoid
        data corruption.

    Also note that not committing will lock the database:

        When a database is accessed by multiple connections, and
        one of the processes modifies the database, the SQLite
        database is locked until that transaction is committed.


    +-------------+   TraceAction*      +-----------+
    | ETL Threads +------------------Q--> EtlTracer |
    | (multiple)  |                     |  Thread   |
    +-------------+                     +-----------+
                                        |  TraceDB  |
                                        +--+--------+
                                           |
                                        +--v--------+
                                        | Database  |
                                        +--+--------+
                                           |
                                           |
    +-------------+                     +--+--------+
    |  Analyze    <---------------------+  TraceDB  |
    |  Threads    |                     | TraceData |
    +-------------+                     +-----------+

    '''

    VERSION='dev'


    CREATE_STATEMENTS = (
        """\
        create table etl(
          state_code  text,
          db_ver      text)
        """,
    )


    def __init__(self, path, mode='r'):

        if mode == 'r':
            self.__readonly = True
            if not os.path.exists(path):
                raise Exception("TraceDB %s doesn't exist" % (path))
        elif mode == 'rw':
            self.__readonly = False
        else:
            raise Exception("mode must be r or rw")

        self.__db = sqlite3.connect(path)
        self.__db_lock = Lock()


    # === sqlite3 DB access methods protected by lock =====================

    def execute_select(self, sql, parms=None, return_dict=True):
        '''
        Execute SQL to select data from the database (no modifications)

        execute_select("""
            select name_last, age
            from people
            where name_last=:who and age=:age
            """,  {"who": who, "age": age})

        Note: theoretically sqlite3 supports multiple cursors open at once,
        but I've had trouble with such.  So this method retrieves all results,
        closes the cursor, and returns the results.
        '''

        with self.__db_lock:
            cursor = self.__db.cursor()
            results = ResultBuffer()

            if parms:
                cursor.execute(sql, parms)
            else:
                cursor.execute(sql)

            for row in cursor:
                if return_dict:
                    d = {}
                    for idx, col in enumerate(cursor.description):
                        d[col[0]] = row[idx]
                    row = d
                results.add(row)

        # Stream back results (lock released)
        for row in results.all():
            yield row


    def execute_update(self, sql, parms=None, commit=True):
        '''Execute SQL that writes to the DB'''
        self.assert_readwrite()
        with self.__db_lock:

            if parms is None:
                self.__db.execute(sql)
            else:
                self.__db.execute(sql, parms)

            if commit:
                self.__db.commit()




    # === Trace DB logic and structure ====================================


    # @property
    # def sqlite3db(self):
    #     if self.__db is None:
    #         raise Exception("Database closed")
    #     return self.__db


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
