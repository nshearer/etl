import os
from sys import stdout, stderr
from threading import Thread, Lock
from zlib import compressobj
from tempfile import mkdtemp
import shutil

from .constants import *
from .exceptions import NoMoreData

from .EtlComponent import EtlComponent
from .EtlInput import EtlInput
from .EtlRecord import EtlRecord
from .EtlOutput import EtlOutput

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


class EtlTracer(Thread):
    '''
    ETL Component added to workflows to trace records being processed to file

    Used to enable status checking and debugging, as well as console logging
    '''

    trace_input = EtlInput()

    def __init__(self, wf_context):

        # Init thread
        super(EtlTracer, self).__init__()

        # Tracer variables
        self.__mute_lock = Lock()
        self.__wf_context = wf_context
        self.__output_tmp_dir = None
        self.__log_fh = None
        self.__trace_file_path = None
        self.__trace_db = None
        self.__started = False

        # Tracer settings
        self.console_log_level = None
        self.console_dev = stdout
        self.error_console_dev = stderr


    def set_trace_path(self, path):
        '''Set the path'''
        with self.__mute_lock:
            if self.__started:
                raise Exception("Can't set trace path after workflow is started")
            self.__trace_file_path = path



    def run(self):

        # Starting

        with self.__mute_lock:

            if self.__started:
                raise Exception("Already started")

            # If trace path specified, open file handles
            if self.__trace_file_path is not None:
                self.__output_tmp_dir = mkdtemp()

                self.__record_trace = GZOutputWriter(os.path.join(self.__output_tmp_dir, 'records.gz'))
                self.__record_index_trace = GZOutputWriter(os.path.join(self.__output_tmp_dir, 'index.gz'))
                self.__log_file = GZOutputWriter(os.path.join(self.__output_tmp_dir, 'log.gz'))

            self.__started = True


        # Tracing
        try:
            for event in self.trace_input.all():

                # TODO: Trace records to disk

                # Print to console
                if self.console_log_level is not None:
                    if event.record_type == 'log':
                        if event['level'] >= self.console_log_level:
                            print(event['message'])

        except NoMoreData:
            pass

            # Close
            # TODO: commit trace data

        finally:
            # Cleanup
            if self.__record_trace is not None:
                self.__record_trace.close()
            if self.__record_index_trace is not None:
                self.__record_index_trace.close()
            if self.__log_file is not None:
                self.__log_file.close()
            if self.__output_tmp_dir is not None:
                if os.path.exists(self.__output_tmp_dir):
                    shutil.rmtree(self.__output_tmp_dir)



class EtlTraceHandle(EtlOutput):
    '''Handle given to other components to interact with the tracer'''

    def __init__(self, tracer):
        super(EtlTraceHandle, self).__init__()
        self.connect(tracer.trace_input)

    def log(self, message, level=LOG_INFO):
        self.output(EtlRecord(
            record_type = 'log',
            level = level,
            message = message))

