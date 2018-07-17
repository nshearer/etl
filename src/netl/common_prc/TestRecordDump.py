
try:
    import ujson as json
except ImportError:
    import json


from netl import EtlComponent, EtlInput
from netl.utils.GZOutputWriter import GZOutputWriter

from netl.EtlRecord import repr_attr_value

class TestRecordDump(EtlComponent):
    '''
    Test sending records to file
    '''

    records = EtlInput()

    def run(self):

        # Open files to dump record stream to
        idx_fh = GZOutputWriter(r'G:\Projects\netl\src\test.trace.recidx')
        recs_fh = GZOutputWriter(r'G:\Projects\netl\src\test.trace.recs')
        envl_fh = GZOutputWriter(r'G:\Projects\netl\src\test.trace.envl')

        # Take incoming records
        for envl in self.records.all(envelope=True):

            # Save envelope
            txt = "\t".join([
                str(envl.from_comp),
                str(envl.from_port),
                str(envl.to_comp),
                str(envl.to_port),
                str(envl.record.serial),
            ]) + "\n"
            envl_fh.write(txt.encode('utf-8'))

            # Save Record
            rec = {
                'serial': str(envl.record.serial),
                'type': str(envl.record.record_type),
                'attrs': {k: repr_attr_value(v) for (k, v) in envl.record.items()},
            }
            rec = json.dumps(rec) + "\n"
            recs_fh.write(rec.encode('utf-8'))

            # Save record length to index
            txt = "%d\n" % (len(rec))
            idx_fh.write(txt.encode('ascii'))


        idx_fh.close()
        recs_fh.close()
        envl_fh.close()