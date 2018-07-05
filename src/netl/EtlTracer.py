'''
ETL Tracer Functionality

This module provides a singleton like interface for the entire workflow to record
what's happening.  The primary goal of this functionality doesn't assist with the
execution of the workflow in anyway, but rather records information useful to
monitoring the workflow, debugging, and tracing how data flows through ETL.

Because many ETL objects want to tace data, I'm avoid passing the tracer to every
component, connection, and data record produced by using a singleton instance.

See https://stackoverflow.com/questions/31875/is-there-a-simple-elegant-way-to-define-singletons

1) The module itself holds all of the singleton members
2) There is a class which proxies access to the module functions just for convience

'''
import os
from threading import Lock
from tempfile import NamedTemporaryFile
import shutil
import sqlite3

from .constants import *

# Tracing settings
ETL_TRACER_SETTINGS = object()
ETL_TRACER_SETTINGS.lock = Lock()
ETL_TRACER_SETTINGS.enabled = False

# Workflow Data
ETL_TRACER_SETTINGS.wf_context = None
ETL_TRACER_SETTINGS.output_tmp_dir = None

# Trace Output
ETL_TRACER_SETTINGS.trace_path = None
ETL_TRACER_SETTINGS.trace_db = None
ETL_TRACER_SETTINGS.keep_trace = False

# Constants
ETL_TRACER_LARGE_RECORD_LIMIT = 1024 * 1024 # 1 MiB
ETL_TRACER_COMMIT_REC_EVERY = 1024

# Counters
ETL_TRACER_SETTINGS.commit_rec_in = ETL_TRACER_COMMIT_REC_EVERY


def setup_tracer(path=None, overwrite=False, keep=None):
    '''
    Setup tracer for receiving data

    :param path: Path to save trace information to (sqlite3 db)
    :param overwrite: If true, will delete existing trace file if exists
    :param keep: If true, will not delete trace file when ETL completes
    '''

    with ETL_TRACER_SETTINGS.lock:

        # Determine path to trace to
        if os.path.exists(path):
            if overwrite:
                os.unlink(path)
            else:
                raise Exception("Trace file already exists: "+path)
        if path is None:
            ETL_TRACER_SETTINGS.trace_path = NamedTemporaryFile(delete=False)
            ETL_TRACER_SETTINGS.trace_path.close()
            ETL_TRACER_SETTINGS.trace_path = path.name

            if keep is None:
                ETL_TRACER_SETTINGS.keep_trace = False
            else:
                ETL_TRACER_SETTINGS.keep_trace = keep
        else:
            ETL_TRACER_SETTINGS.trace_path = path

            if keep is None:
                ETL_TRACER_SETTINGS.keep_trace = True
            else:
                ETL_TRACER_SETTINGS.keep_trace = keep

        # Create DB file to trace to
        ETL_TRACER_SETTINGS.trace_db = sqlite3.connect(ETL_TRACER_SETTINGS.trace_path)
        db = ETL_TRACER_SETTINGS.trace_db

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
            create table records (
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
        ETL_TRACER_SETTINGS.enabled = True


def stop_tracer():
    '''
    Close the trace DB
    '''

    with ETL_TRACER_SETTINGS.lock:
        ETL_TRACER_SETTINGS.enabled = False
        ETL_TRACER_SETTINGS.trace_db.close()
        ETL_TRACER_SETTINGS.trace_db = None
        if not ETL_TRACER_SETTINGS.keep_trace:
            os.unlink(ETL_TRACER_SETTINGS.trace_path)


class EtlTracer:
    '''Tracer logic that uses the global trace variables'''

    def check_enabled(self):
        '''Check to see if tracing is enabled'''
        if ETL_TRACER_SETTINGS.enabled:
            if ETL_TRACER_SETTINGS.trace_db is not None:
                return True

    def new_component(self, component_id, name, clsname, state):
        '''
        Tell the tracer about a component

        :param component_id: Unique, integer ID for this component
        :param name: Name of the component
        :param clsname: Class name of the component
        :param state: Code that represents the state of the component
        '''
        with ETL_TRACER_SETTINGS.lock:
            if self.check_enabled():
                db = ETL_TRACER_SETTINGS.trace_db
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
        with ETL_TRACER_SETTINGS.lock:
            if self.check_enabled():
                db = ETL_TRACER_SETTINGS.trace_db
                db.cursor().execute("""
                    update components
                    set state = ?
                    where id = ?
                    """, (state, int(component_id)))
                db.commit()


