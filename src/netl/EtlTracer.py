import os
from threading import Lock
from queue import Queue
from tempfile import NamedTemporaryFile
import sqlite3
from threading import Thread
from pprint import pformat
import traceback

from .constants import *


class EtlTracer(Thread):
    '''
    Tracks the progress of the ETL flow
    '''
    
    # Constants
    LARGE_RECORD_LIMIT = 1024 * 1024 # 1 MiB
    COMMIT_REC_EVERY = 1024    

    # STATE CONSTANTS
    INIT_STATE = 'init'
    RUNNING_STATE = 'running'
    FINISHED_STATE = 'finshed'
    ERROR_SATE = 'error'

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

        # Trace Data
        self.__trace_db = None

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

                # Create DB file to trace to
                self.__trace_db = sqlite3.connect(self.__trace_path)
                db = self.__trace_db

                # ETL Status Table: Single record representing status of the ETL
                db.cursor().execute("""
                    create table status(
                      state_code  text)
                """)

                # Component Status Table: One record per component in the ETL
                db.cursor().execute("""
                    create table components (
                      id          int primary key,
                      name        text,
                      class       text,
                      state_code  text)
                """)

                # Component Port Table: One record per component input or output port
                db.cursor().execute("""
                    create table component_ports (
                      comp_id     int,
                      id          int primary key,
                      name        text,
                      state_code  text)
                """)

                # Record Data Table: One record per component in the ETL
                db.cursor().execute("""
                    create table records (
                      id                int primary key,
                      origin_component  int,
                      large_record      int,
                      data              text)
                """)

                # Record Trace Table: Records are sent from one component to another
                db.cursor().execute("""
                    create table envelopes (
                      id                int primary key,
                      record_id         int,
                      from_port_id      int,
                      to_port_id        int)
                """)

                # Record Derivation Trace Table: Trace when one record value is referenced to calucate the value of another
                db.cursor().execute("""
                    create table record_derivation (
                      ref_record_id     int,
                      ref_record_attr   text,
                      calc_record_id    int,
                      calc_record_attr  text)
                """)

                # Component Status Table: Large record storage
                db.cursor().execute("""
                    create table large_records (
                      id                int primary key)
                """)
                db.cursor().execute("""
                    create table large_record_data (
                      large_rec_id      int,
                      chunk_num         int,
                      data              text)
                """)

                # Allow trace operations to start
                self.__configured = True

                # Close trace DB.  Will need to open in thread
                self.__trace_db.close()
                self.__trace_db = None

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
            if self.__trace_db is not None:
                raise Exception("Trace Database alrady open")
            self.__trace_db = sqlite3.connect(self.__trace_path)

            # Else, watch the input queue and respond
            while True:
                event = self.__trace_queue.get()

                try:

                    code = event['event']
                    del event['event']

                    if code == 'new_component':
                        self.__new_component(**event)
                    elif code == 'component_state_change':
                        self.__component_state_change(**event)
                    elif code == 'stop':
                        self.logger.debug("Got stop command")
                        self.__trace_db.close()
                        self.__trace_db = None
                        return
                    else:
                        self.logger.error("Got invalid trace code '%s': %s" % (code, pformat(event)))

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
                'event':        'new_component',
                'component_id': component_id,
                'name':         name,
                'clsname':      clsname,
                'state':        state,
                })
    def __new_component(self, component_id, name, clsname, state):
        db = self.__trace_db
        db.cursor().execute("""
            insert into components (id, name, class, state_code)
            values (?, ?, ?, ?)
            """, (int(component_id), name, clsname, state))
        db.commit()


    def component_state_change(self, component_id, state):
        '''
        Update the status of the component

        :param component_id: nique, integer ID for this component
        :param state: New state code
        '''
        if self.tracing_running:
            self.__trace_queue.put({
                'event':        'component_state_change',
                'component_id': component_id,
                'state':        state,
                })
    def __component_state_change(self, component_id, state):
        db = self.__trace_db
        db.cursor().execute("""
            update components
            set state_code = ?
            where id = ?
            """, (state, int(component_id)))
        db.commit()


    def trace_record_rcvd(self, envilope):
        # TODO: Trace envilopes
        print(str(envilope))

