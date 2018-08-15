import os, sys
from threading import Lock
from queue import Queue
from tempfile import NamedTemporaryFile
from threading import Thread, Lock
from pprint import pformat
import traceback
from datetime import datetime, timedelta
from random import randint

from .tracefile import TraceDumpFileWriter, ComponentTrace

class EtlTracer(Thread):
    '''
    Tracks the progress of the ETL flow
    '''
    
    # Constants
    AUTO_COMMIT_EVERY = timedelta(seconds=5)

    def __init__(self):

        # Thread controls
        super(EtlTracer, self).__init__(name='EtlTracer')
        self.__lock = Lock()
        self.__configured = False
        self.__trace_queue = Queue(maxsize=4096)

        # Tracer settings
        self.__trace_path = None
        self.__overwrite = None
        self.__keep_trace = None

        # Auto commit tracking
        self.__next_autocommit = None

        self.logger = None # Will get passed in from EtlSession


    @property
    def tracing_enabled(self):
        return self.__configured


    @property
    def tracing_running(self):
        return self.__configured and self.isAlive()


    def setup_tracer(self, path, overwrite=False, keep=None):
        '''
        Setup tracer to begin receiving data

        :param path: Path to save trace information to
        :param overwrite: If true, will delete existing trace file if exists
        :param keep: If true, will not delete trace file when ETL completes
        '''
        with self.__lock:

            try:

                # Determine path to trace to
                if os.path.exists(path):
                    if overwrite:
                        os.unlink(path)
                    else:
                        raise Exception("Trace file already exists: "+path)

                self.__trace_path = path

                if keep is None:
                    self.__keep_trace = True
                else:
                    self.__keep_trace = keep

                # Allow trace operations to start
                self.__configured = True

            except Exception as e:
                raise Exception("Failed to setup trace file: %s" % (str(e)))
                # self.__configured = False
                # self.__keep_trace = True
                # self.__trace_path = None


    def should_auto_commit(self):
        '''
        Should we commit our changes.

        Auto commit looks at the number of incoming messages and tries to commit
        every so often.

        :return: bool
        '''

        if self.__next_autocommit is None:
            commit = True

        elif datetime.now() >= self.__next_autocommit:
            commit = True

        # Peak at the queue to see if more trace events are coming
        elif self.__trace_queue.empty():
            commit = True

        else:
            commit = False

        if commit:
            self.__next_autocommit = datetime.now() + self.AUTO_COMMIT_EVERY
            #self.logger.debug("COMMIT")
        return commit


    def run(self):
        '''Thread execution'''
        with self.__lock:

            # Check to see if we're configured to trace anything
            if not self.__configured:
                return

            # Open Trace DB
            tracefile = TraceDumpFileWriter(self.__trace_path)

            # Watch the input queue and respond
            while True:
                event = self.__trace_queue.get()

                try:

                    if event['event'] == 'activity':
                        tracefile.write(
                            entry_code = event['activity'].__class__.__name__,
                            data = event['activity'].data_json,
                            flush = self.should_auto_commit())

                    elif event['event'] == 'stop':
                        self.logger.debug("Got stop command")
                        tracefile.close()
                        return

                    else:
                        self.logger.error("Got invalid trace code '%s': %s" % (event['event'], pformat(event)))

                except Exception as e:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    emsg = traceback.format_exception(exc_type, exc_value, exc_traceback)
                    emsg.append("Encountered %s: %s with trace event %s\n %s" % (
                        e.__class__.__name__, str(e), pformat(event),
                        "".join(emsg)))
                    self.logger.error("\n".join(emsg))



    def stop_tracer(self):
        '''
        Stop tracer supervisor thread and close the trace DB
        '''
        if self.tracing_running:
            self.__trace_queue.put({
                'event': 'stop'
            })
            self.join()
            with self.__lock:
                if not self.__keep_trace:
                    os.unlink(self.__trace_path)
    

    # === Tracing methods ===================================================


    def trace(self, activity):
        if self.tracing_running:
            self.__trace_queue.put({
                'event': 'activity',
                'activity': activity,
            })


    def trace_record_rcvd(self, envilope):
        # TODO: Trace envilopes
        print(str(envilope))

