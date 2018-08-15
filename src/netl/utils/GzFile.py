import codecs
from zlib import compressobj, decompressobj, Z_SYNC_FLUSH
from time import sleep


from io import BufferedIOBase

class GzFile(BufferedIOBase):
    '''
    File handle which reads and writes
    '''

    def __init__(self, path, mode, tail=False):
        '''
        :param path: Path to file
        :param mode: r or w (all operations will be binary)
        :param tail:
            If true, keep reading file even if not bytes remaining
            in case another process is still writing to the file
        '''

        if mode not in ('r', 'w'):
            raise Exception("Mode must be r or w")

        self.__mode = mode
        self.__path = path
        self.__fh = open(path, self.__mode + 'b')
        self.__closed = False
        self.__tailling = tail

        if self.__mode == 'w':
            self.compressor = compressobj()
        else:
            self.compressor = decompressobj()


    def __repr__(self):
        return 'GzFile(r"%s", "%s")' % (self.__path, self.__mode)


    def __asssert_not_clossed(self):
        if self.__closed:
            raise ValueError("File is clossed")


    def close(self):
        if not self.__closed:
            # Flush remaining data
            if self.__mode == 'w':
                self.__fh.write(self.compressor.flush())
            self.compressor = None
            self.__fh.close()
            self.__fh = None
            self.__closed = True
            self.__pos = 0 # Position in file relative to uncompressed data


    @property
    def closed(self):
        return self.__closed


    def fileno(self, *args, **kwargs):
        raise OSError("GzFile has no fileno")

    def __assert_reading(self):
        self.__asssert_not_clossed()
        if self.__mode != 'r':
            raise ValueError("Operation not valid in mode %s" % (self.__mode))


    def __assert_writing(self):
        self.__asssert_not_clossed()
        if self.__mode != 'w':
            raise ValueError("Operation not valid in mode %s" % (self.__mode))


    def read(self, size=-1):
        self.__assert_reading()
        if size == -1:
            return self.readall()
        while True:
            compressed = self.__fh.read(size)
            if compressed:
                decompressed = self.compressor.decompress(compressed)
                if decompressed:
                    return decompressed

            if self.__tailling:
                sleep(5)
            else:
                return b''


    def readall(self):
        self.__assert_reading()
        def _yieled_readall():
            while True:
                decompressed = self.read(4096)
                if decompressed:
                    yield decompressed
                else:
                    break
        return b''.join(_yieled_readall())


    def write(self, data):
        self.__assert_writing()
        data = self.compressor.compress(data)
        self.__fh.write(data)


    def flush(self):
        '''Ensure all data is dumped out of the comprssor'''
        self.__assert_writing()
        if self.__mode == 'w':
            self.__fh.write(self.compressor.flush(Z_SYNC_FLUSH))
            self.__fh.flush()

        else:
            raise ValueError("Can't flush() unless writting")

    def isatty(self):
        return False

    def readable(self):
        self.__asssert_not_clossed()
        return self.__mode == 'r'

    def writable(self):
        self.__asssert_not_clossed()
        return self.__mode == 'w'

    def seekable(self, *args, **kwargs):
        self.__asssert_not_clossed()
        return False

    def seek(self, offset, whence):
        self.__asssert_not_clossed()
        raise ValueError("Can't seek on GzFile")

    def tell(self):
        self.__asssert_not_clossed()
        return self.__pos

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def open_gz(path, mode, encoding='utf-8', tail=False):
    '''Open a file that is compressed with zlib'''

    if mode in ('rt', 'r'):
        return codecs.getreader(encoding)(GzFile(path, 'r', tail))
    elif mode == 'rb':
        return GzFile(path, 'r', tail)
    elif mode in ('wt', 'w'):
        return codecs.getwriter(encoding)(GzFile(path, 'w'))
    elif mode == 'wb':
        return GzFile(path, 'w')
    else:
        raise ValueError("Invalid mode: " + mode)
