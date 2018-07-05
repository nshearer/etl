import os
from threading import Lock
from tempfile import NamedTemporaryFile
import shutil
import sqlite3

from .constants import *


class EtlTracer:
    '''
    Tracks the progress of the ETL flow
    '''
    
    # Constants
    LARGE_RECORD_LIMIT = 1024 * 1024 # 1 MiB
    COMMIT_REC_EVERY = 1024    
    
    def __init__(self):

        self.lock = Lock()
        self.enabled = False

        # Tracer settings
        self.path = None
        self.overwrite = None
        self.keep_trace = None

        # Workflow Data
        self.wf_context = None
        self.output_tmp_dir = None
        
        # Trace Output
        self.trace_path = None
        self.trace_db = None
        self.keep_trace = False
        
        # Counters
        self.commit_rec_in = self.COMMIT_REC_EVERY

        
    def setup(self, path=None, overwrite=False, keep=None):
        '''
        Setup tracer to begin receiving data

        :param path: Path to save trace information to (sqlite3 db)
        :param overwrite: If true, will delete existing trace file if exists
        :param keep: If true, will not delete trace file when ETL completes
        '''
        with self.lock:

            # Determine path to trace to
            if os.path.exists(path):
                if overwrite:
                    os.unlink(path)
                else:
                    raise Exception("Trace file already exists: "+path)
            if path is None:
                self.trace_path = NamedTemporaryFile(delete=False)
                self.trace_path.close()
                self.trace_path = path.name
    
                if keep is None:
                    self.keep_trace = False
                else:
                    self.keep_trace = keep
            else:
                self.trace_path = path
    
                if keep is None:
                    self.keep_trace = True
                else:
                    self.keep_trace = keep
    
            # Create DB file to trace to
            self.trace_db = sqlite3.connect(self.trace_path)
            db = self.trace_db
    
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
            self.enabled = True
    
    
    def stop(self):
        '''
        Close the trace DB
        '''
    
        with self.lock:
            self.enabled = False
            self.trace_db.close()
            self.trace_db = None
            if not self.keep_trace:
                os.unlink(self.trace_path)
    

    # === Tracing methods ===================================================


    def check_enabled(self):
        '''Check to see if tracing is enabled'''
        if self.enabled:
            if self.trace_db is not None:
                return True

    def new_component(self, component_id, name, clsname, state):
        '''
        Tell the tracer about a component

        :param component_id: Unique, integer ID for this component
        :param name: Name of the component
        :param clsname: Class name of the component
        :param state: Code that represents the state of the component
        '''
        with self.lock:
            if self.check_enabled():
                db = self.trace_db
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
        with self.lock:
            if self.check_enabled():
                db = self.trace_db
                db.cursor().execute("""
                    update components
                    set state = ?
                    where id = ?
                    """, (state, int(component_id)))
                db.commit()


