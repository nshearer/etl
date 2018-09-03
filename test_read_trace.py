import json
from pprint import pprint
from datetime import datetime

from netl.utils.GzFile import GZOutputReader


class TraceRecordReader:

    def __init__(self, idx_path, rec_path):
        self.idx_fh = GZOutputReader(idx_path)
        self.recs_fh = GZOutputReader(rec_path)


    def records(self):

        for line in self.idx_fh.readlines():
            rec_len = int(line.decode('ascii').strip())
            record_data = self.recs_fh.read(rec_len)
            yield json.loads(record_data.decode('utf-8'))




if __name__ == '__main__':

    records = TraceRecordReader(
        idx_path = r"G:\Projects\netl\src\test.trace.recidx",
        rec_path = r"G:\Projects\netl\src\test.trace.recs")

    started = datetime.now()
    for record in records.records():
        pass
        pprint(record, indent=4)
    ended = datetime.now()

    print("Took %s" % (str(ended - started)))

