

class TraceObject:
    '''An object stored in the trace database'''

    def __init__(self, db):
        self.trace_db = db


    @property
    def sqlite3db(self):
        return self.trace_db.sqlite3db


    def assert_readwrite(self):
        if self.trace_db.__readonly:
            raise Exception("Database open for read only")


