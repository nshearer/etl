from tempfile import TemporaryFile
from pickle import dumps, loads, HIGHEST_PROTOCOL

class ResultBuffer:
    '''
    Buffer results in a way that's memory safe

    Written primarily to allow Trace DB to retrieve all results before yielding
    without risking using too much memory.  Will start buffering to disk if needed.
    '''

    DEFAULT_LIMIT = 10000

    def __init__(self, container=None, limit=DEFAULT_LIMIT):
        '''
        :param container: If provided, then will get all items from container
        '''
        self.__items = list()
        self.__limit = limit
        self.__reading_started = False

        self.__data_fh = None
        self.__index_fh = None

        if container is not None:
            for item in container:
                self.add(item)

    def add(self, item):
        '''
        Add another item to the result set

        :param item: Anything that can be pickled
        '''

        if self.__reading_started:
            raise Exception("Can't add items after started reading them back")

        self.__items.append(item)

        # Save to disk if needed
        if len(self.__items) >= self.__limit:

            # Open files
            if self.__data_fh is None:
                self.__data_fh = TemporaryFile(mode='r+b')
                self.__index_fh = TemporaryFile(mode='r+t')

            # Seriealize
            chunk = dumps(self.__items, HIGHEST_PROTOCOL)

            # Save to disk
            self.__data_fh.write(chunk)
            self.__index_fh.write(str(len(chunk))+"\n")

            # Reset collection for next item
            self.__items = list()


    def all(self):
        '''
        Get all the results back
        '''

        # Start Readback
        if self.__reading_started:
            raise Exception("Can't read back twice")
        self.__reading_started = True

        if self.__data_fh is not None:
            self.__data_fh.seek(0)
            self.__index_fh.seek(0)

        # If files were used, return from there first
        if self.__index_fh is not None:
            for chunk_size in self.__index_fh:
                chunk = self.__data_fh.read(chunk_size)
                chunk = loads(chunk)
                for item in chunk:
                    yield item

            # Clean up files
            self.__data_fh.close()
            self.__index_fh.close()

        # Return in-memory items
        for item in self.__items:
            yield item

