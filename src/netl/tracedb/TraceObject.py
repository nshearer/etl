

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


def row_dict_factory(cursor, row):
    '''Convert sqlite3 result row into a dict'''
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

