import os
from zlib import compressobj


class GZOutputWriter:
    '''
    File output that is compresses with gzip
    '''

    def __init__(self, path):
        self.path = path
        self.fh = open(path, 'wb')
        self.compressor = compressobj()

    def write(self, data, flush=False):
        if self.compressor is None:
            raise Exception("File closed")
        data = self.compressor.compress(data)
        if flush:
            data += self.compressor.flush()
        self.fh.write(data)
        if flush:
            self.fh.flush()
            os.fsync(self.fh.fileno())

    def close(self):
        self.fh.write(self.compressor.flush())
        self.compressor = None
        self.fh.close()
        self.fh = None
