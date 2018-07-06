import os
from threading import Lock
from queue import Queue
from tempfile import NamedTemporaryFile
import sqlite3
from threading import Thread
from pprint import pformat
import traceback

from .tracedb import TraceDB

class EtlTracer(Thread):
    '''
    Tracks the progress of the ETL flow
    '''
    
    # Constants
    LARGE_RECORD_LIMIT = 1024 * 1024 # 1 MiB
    COMMIT_REC_EVERY = 1024    

    # STATE CONSTANTS
    INIT_STATE = TraceDB.INIT_STATE
    RUNNING_STATE = TraceDB.RUNNING_STATE
    FINISHED_STATE = TraceDB.FINISHED_STATE
    ERROR_SATE = TraceDB.ERROR_SATE

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

        # Counters
        self.__commit_rec_in = self.COMMIT_REC_EVERY

        self.logger = None # Will get passed in from EtlSession


    @property
    def tracing_enabled(self):
        return self.__configured


    @property
    def tracing_running(self):
        return self.__configured and self.isAlive()


    def setup_tracer(self, path=None, overwrite=False, keep=None):
        '''
        Setup tracer to begin receiving data

        :param path: Path to save trace information to (sqlite3 db)
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
                if path is None:
                    self.__trace_path = NamedTemporaryFile(delete=False)
                    self.__trace_path.close()
                    self.__trace_path = path.name

                    if keep is None:
                        self.__keep_trace = False
                    else:
                        self.__keep_trace = keep
                else:
                    self.__trace_path = path

                    if keep is None:
                        self.__keep_trace = True
                    else:
                        self.__keep_trace = keep

                TraceDB.create(path)

                # Allow trace operations to start
                self.__configured = True

            except Exception as e:
                self.logger.error("Failed to setup trace DB: %s" % (str(e)))
                self.__configured = False
                self.__keep_trace = True
                self.__trace_path = None


    def run(self):
        '''Thread execution'''
        with self.__lock:

            # Check to see if we're configured to trace anything
            if not self.__configured:
                return

            # Open Trace DB
            db = TraceDB(self.__trace_path, mode='rw')

            # Watch the input queue and respond
            while True:
                event = self.__trace_queue.get()

                try:
                    code = event['event']
                    del event['event']

                    if code == 'stop':
                        self.logger.debug("Got stop command")
                        db.close()
                        return

                    else:
                        trace_func = getattr(db, code)
                        trace_func(**event)

                    # else:
                    #     self.logger.error("Got invalid trace code '%s': %s" % (code, pformat(event)))

                except Exception as e:
                    emsg = traceback.format_stack()
                    emsg.append("Encountered %s: %s" % (e.__class__.__name__, str(e)))
                    self.logger.error("Encountered exception when executing trace code %s: %s\n%s" % (
                        code, pformat(event), "\n".join([s.strip() for s in emsg])+"\n"))



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

    def new_component(self, component_id, name, clsname, state):
        '''
        Tell the tracer about a component

        :param component_id: Unique, integer ID for this component
        :param name: Name of the component
        :param clsname: Class name of the component
        :param state: Code that represents the state of the component
        '''
        if self.tracing_running:
            self.__trace_queue.put({
                'event':        'trace_new_component',
                'component_id': component_id,
                'name':         name,
                'clsname':      clsname,
                'state':        state,
                })


    def component_state_change(self, component_id, state):
        '''
        Update the status of the component

        :param component_id: nique, integer ID for this component
        :param state: New state code
        '''
        if self.tracing_running:
            self.__trace_queue.put({
                'event':        'trace_component_state_change',
                'component_id': component_id,
                'state_code':   state,
                })


    def trace_record_rcvd(self, envilope):
        # TODO: Trace envilopes
        print(str(envilope))

