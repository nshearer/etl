from threading import Thread
import logging
from datetime import datetime

try:
    import ujson as json
except ImportError:
    import json


from .TraceData import TraceData
from .all_trace_events import ALL_TRACE_EVENTS

from ..utils.GzFile import open_gz

class TraceDumpFileWriter:
    '''
    Dumps activity information to a file to examine how the ETL ran

    File Format:

        The dump file is a zlib compressed text file

        A newline is always added to the end of the record data so that
        the file can be efficiently read back as a set of lines.

        Each item is written to the file as a set of lines:

            C #
            JSON_DATA

        Where

            C is a code to specify what type of data is being written
            # is the number of lines in RECORD_DATA
            JSON_DATA is the actual data

        Example:

            T 1
            {'severity': 'INFO', 'msg': 'Starting component "Extract"'}


    '''

    VERSION='dev'

    TRACE_EVENT='T'
    END_OF_FILE_EVENT='E'

    def __init__(self, path):
        self.__fh = open_gz(path, 'wt')


    def _write_entry(self, item_type, data, flush):
        '''
        Write a record to the file
        :param event_code: Single character item type
        :param data: Data to be saved to disk
        :param flush: Should data be flushed to disk
        '''

        # Count the lines in the data
        data = json.dumps(data) + "\n"

        # Build header
        header = "{code} {count}\n".format(code=item_type, count=data.count("\n"))

        # Write out
        self.__fh.write(header)
        self.__fh.write(data)
        if flush:
            self.__fh.flush()


    def write_trace_event(self, event, flush=False):
        '''
        Add a trace event data to the file

        :param entry_code: A string to identify the type of entry being saved
        :param event: Event being saved
        '''

        data = {
            'event_class': event.__class__.__name__,
            'event_attrs': {k: getattr(event, k) for k in event.data_keys},
            'ts': event.ts.strftime("%c"),
        }
        self._write_entry(self.TRACE_EVENT, data, flush=flush)


    def close(self):
        '''
        Close the file
        '''
        self._write_entry(entry_code=self.END_OF_FILE_EVENT,
                          data='END OF FILE')
        self.__fh.close()
        self.__fh = None


    @staticmethod
    def rebuild_trace_event(data):
        for event_cls in ALL_TRACE_EVENTS:
            if data['event_class'] == event_cls.__name__:
                event = event_cls(**data['event_attrs'])
                event.ts = datetime.strptime(data['ts'], "%c")
                return event

        raise Exception("Didn't find a TraceEvent class called %s in all_trace_events.ALL_TRACE_EVENTS" % (
            data['event_class']))



class TraceDumpFileReader:
    '''
    Reads back items written by TraceDuempFileWriter
    '''

    def __init__(self, path):
        self.__path = path
        self.__log = logging.getLogger('etl.TraceReader')


    def all(self):
        '''
        Reads through all the items and yields them back

        Note:
         - reopens the file each time, so you can call multiple times
         - gracefully handles end of file, so you can read it while it's
           being written out elsewhere.

        :return: yields item_code, item_data
        '''

        with open_gz(self.__path, 'rt', tail=True) as fh:

            cur_item_code = None
            cur_num_lines = None
            data_lines = None

            def _readlines():
                while True:
                    yield fh.readline()

            for i, line in enumerate(_readlines()):

                # Process header
                if cur_item_code is None:
                    try:
                        cur_item_code, cur_num_lines = line.strip().split(" ")
                        cur_num_lines = int(cur_num_lines)
                        data_lines = list()
                    except ValueError:
                        self.__log.error("Couldn't parse header line %d" % (i+1))

                # Collect data
                else:
                    data_lines.append(line)

                # Process data
                if len(data_lines) == cur_num_lines:

                    # Decode data
                    try:
                        data = json.loads(("\n".join(data_lines)))
                    except Exception as e:
                        self.__log.error("\n".join([
                            "Couldn't decode data on line %d" % (i+1),
                            "Error: " + str(e),
                            "Data:",
                            "\n".join(data)
                        ]))
                        data = None

                    # Return data
                    if data is not None:
                        yield cur_item_code, data

                    # Reset for next record
                    cur_item_code = None
                    cur_num_lines = None
                    data_lines = None


class TraceFileMonitor(Thread):
    '''
    Worker class that watches a trace file and compiles into TraceData
    '''

    def __init__(self, path):
        '''
        :param path: Path to trace file
        '''
        self.data = TraceData()
        self.__path = path
        super(TraceFileMonitor, self).__init__(
            name='TraceDataWatcher',
            daemon=True)


    def run(self):
        '''Monitor trace file'''
        reader = TraceDumpFileReader(self.__path)
        for code, data in reader.all():
            if code == TraceDumpFileWriter.TRACE_EVENT:

                event = TraceDumpFileWriter.rebuild_trace_event(data)

                self.data.lock.acquire_write()
                try:
                    event.apply_to_trace_data(self.data)
                finally:
                    self.data.lock.release_write()

