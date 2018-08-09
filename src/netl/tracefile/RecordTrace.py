from hashlib import md5
import struct

from .TraceData import TraceData

class TraceRecord(TraceData):
    '''A record in the ETL'''

    def _list_required_keys(self):
        return (
            'type',     # Record type code
            'serial',   # Unique ID of the record
            'attrs',    # repr of the record values
        )

