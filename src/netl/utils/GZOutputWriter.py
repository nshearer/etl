import os
from zlib import compressobj, decompressobj


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



class GZOutputReader:
    '''
    Read back file written by GZOutputWriter
    '''

    def __init__(self, path):
        self.path = path
        self.fh = open(path, 'rb')
        self.decompressor = decompressobj()

        self.__tail_data = b""


    def read_all(self):
        '''
        Read all data back (yields chunk iterativly)
        '''
        if self.decompressor is None:
            raise Exception("File closed")

        data = self.fh.read(4096)
        while data:
            data = self.decompressor.decompress(data)
            if data:
                yield data
            data = self.fh.read(4096)

        yield self.decompressor.flush()
        self.close()


    def read(self, bytes=None):
        '''
        Read a chunk of data from the

        :param bytes: If specified, then return this many bytes from the stream
        :return: bytes
        '''
        if self.decompressor is None:
            return None

        if bytes is None:
            return b"".join(self.read_all())

        # Fill tail data
        if len(self.__tail_data) < bytes:
            for chunk in self.read_all():
                self.__tail_data += chunk
                if len(self.__tail_data) >= bytes:
                    break

        # Return bytes requested
        rtn = self.__tail_data[:bytes]
        self.__tail_data = self.__tail_data[bytes:]
        return rtn


    def readlines(self):
        '''Yield all line one at a time'''

        if self.decompressor is None:
            return

        tail = b""
        for chunk in self.read_all():
            tail += chunk
            while True:
                pos = tail.find(b"\n") + 1
                if pos != 0:
                    yield tail[:pos]
                    tail = tail[pos:]
                else:
                    break

        while True:
            pos = tail.find(b"\n") + 1
            if pos != 0:
                yield tail[:pos]
                tail = tail[pos:]
            else:
                break

        if len(tail) > 0:
            yield tail


    def close(self):
        self.decompressor = None
        self.fh.close()
        self.fh = None

