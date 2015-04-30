from threading import Thread, Lock
from Queue import Queue, Empty

from abc import ABCMeta, abstractmethod

from EtlEvent import InputRecordRecieved
from PostRecordProcessingAction import RecordConsumed

import ports

from EtlBuildError import EtlBuildError


class EtlProcessorBase(object):
    '''Base class for EtlProcessor
    
    Each Processor goes through these states.  The current state can be queried
    by the current_state property.

    SETUP_PHASE       - Is the phase before processor is started.  This is the
                        the processor starts in, and is meant to provide time to
                        configure the component prior to starting the ETL process.

    STARTUP_PHASE     - Is the state that the processor enters while starting the
                        ETL process, before the processor starts reciving or
                        dispatching records.

    PAUSED            - Temporary state to stop processing

    RUNNING_PHASE     - Is the state that the processor is in while it is 
                        processing (recieving and dispatching) records.

    FINSIHED_PHASE    - Is the status the the processor is in when it will no
                        longer recieve or dispatch records.


                    +-------+   start_processor()   +---------+
                    | SETUP +-----------------------> STARTUP |
                    +-------+                       +----+----+
                                                         |     
                                                   after |     
                                     starting_processor()|     
                                                    call |     
                                                         |     
                   +--------+   pause_processor()   +----v----+
                   | PAUSED <-----------------------> RUNNING |
                   +--------+  resume_processor()   +----+----+
                                                         |     
                                            after inputs |     
                                             and outputs |     
                                              all closed |     
                                                         |     
                                                   +-----v----+
                                                   | FINISHED |
                                                   +----------+    
    

    Because Processors have multiple stages, and run in threads, knowing
    which method can be called when gets complex.  Here is the convention
    used to keep information organized:

    | Name  |       Desc          |   Called by       |
    |-------|---------------------|-------------------|
    | df_*  | Definition Methods  | Self during setup |
    | st_*  | Static/Thread Safe  | Anyone            |
    | if_*  | Interface           | Other Processors  |
    | ct_*  | Control             | Parent Processor  |
    | pr_*  | Processing          | Inside Prc Thread |

    Each method may also check to verify that it is only called in specific
    phases by calling one of the _*_phase_method() methods as the first line
    of the method.  This serves both to remember when the method can be
    called, and to enforce.


    |       Method       | SETUP | STARTUP | RUNNING | FINISHED |
    |--------------------|-------|---------|---------|----------|
    | create_input_port  |   *   |         |         |          |
    | create_output_port |   *   |         |         |          |
    | _lock_input_port   |   *   |         |         |          |
    | _unlock_input_port |       |   *     |         |          |


    Interface methods interact with the internal thread safe queue to allow
    external processors (or any external objects) to send signals/records to
    this processor to work on.  Unlike previous versions of this ETL, external
    objects do not push objects into the queue directly.  This is to help
    keep the definition of the "Event" next to the handler for that event.
    That is, you don't have an Event object, that needs to have the same
    parameters as the handling method.  So, in general:

    1) An external method calls an if_* method like if_receive_input()
       using that methods normal signature.

    2) The interface method describes that call with an object and 
       queues it to the thread safe event_queue
       
    3) pr_process_events() picks up that call description object from
       the queue and calls the pr_* version of the interface method,
       such as pr_receive_input().

        +----------------------------------------------------------+    
        |                                |                         |    
        |           if_<name>(args) +----------------> Queue       |    
        |                                |               +         |
        |                                |               |         |    
        | (outside thread)               |               |         |    
        |--------------------------------+               |         |    
        | (inside thread)                                |         |    
        |                                                v         |    
        |           pr_<name>(args) <------------+ pr_event_loop() |
        +----------------------------------------------------------+
                

    Sub Processors
    --------------

    While typically, only the root processor will have children processors,
    support is built into this base class for keeping track of children
    processors.

    The parent processor of a child processor is responsible for tracking
    the status of all of it's children, and not considering itself finished
    until all of the child processes are finished.

    In general, connections can be formed between:

      - Two processes that are sblings of the same parent with the
        df_connect_sib_processors() method
      - From a parent output down to one of it's children's input
        ports with df_connect_parent_to_child()
      - From one of a parent processor's children's output up to a parent
        input port using df_connect_child_to_parent()

    Child processors are added to this processor by calling
    df_add_child_processor().  

    @see EtlProcessor
    '''
    __metaclass__ = ABCMeta


    SETUP_PHASE = 1
    STARTUP_PHASE = 2
    RUNNING_PHASE = 3
    FINISHED = 4

    STATE_DESC = {
        SETUP_PHASE:    'Setup',
        STARTUP_PHASE:  'Startup',
        RUNNING_PHASE:  'Running',
        FINISHED:       'Finished',
    }
    def _state_code_desc(self, code):
        try:
            return self.STATE_DESC[code]
        except IndexError:
            return "UNKNOWN CODE STATE CODE '%s'" % (code)

    
    MAX_EVENT_Q_SIZE = 1000
    MAX_RECORD_Q_SZIE = 100


    def __init__(self, name):
        self.__name = name
        self.__state = self.SETUP_PHASE

        self.input_queue = Queue(maxsize=self.MAX_EVENT_Q_SIZE)
        
        self._input_ports = ports.InputPortCollection()
        self._output_ports = ports.OutputPortCollection()
        self._sub_processors = dict()

        # # Record all input ports
        # for port in self.prc.list_inputs():
        #     if self.__input_ports.has_key(port.name):
        #         raise EtlBuildError(
        #             prc_name = self.prc_name,
        #             prc_class_name = self.prc,
        #             error_msg = "input %s defined twice" % (port.name))
        #     self.__input_ports[port.name] = port
        #     self.__inputs[port.name] = list()
             
        # # Record all output ports
        # for port in self.prc.list_outputs():
        #     if self.__output_ports.has_key(port.name):
        #         raise EtlBuildError(
        #             prc_name = self.prc_name,
        #             prc_class_name = self.prc,
        #             error_msg = "output %s defined twice" % (port.name))
        #     self.__output_ports[port.name] = port
        #     self.__outpus[port.name] = list()
         
        # # Setup record input queues
        # for name in self.__inputs.keys():
        #     self.__input_queues[name] = Queue(maxsize=self.MAX_RECORD_Q_SZIE)
        #     self.__held_records[name] = None


    # -- State Checking ------------------------------------------------------

    def _setup_phase_method(self):
        '''Checks that the method call is happening during setup'''
        self._asert_phase_is(self.SETUP_PHASE)


    def _startup_phase_method(self):
        '''Checks that the method call is happening during startup'''
        self._asert_phase_is(self.STARTUP_PHASE)


    def _processing_phase_method(self):
        '''Checks that the method call is happening during processing'''
        self._asert_phase_is(self.SETUP_PHASE)


    def _finished_phase_method(self):
        '''Checks that the method call is happening when finished'''
        self._asert_phase_is(self.FINISHED_PHASE)


    @property
    def current_state(self):
        '''Current state of this processor'''
        return self.__state


    @property
    def current_state_desc(self):
        '''Friendly description of current state'''
        return self._state_code_desc(self.current_state)


    def _asert_phase_is(self, expected_state):
        if self.__state != expected_state:
            msg = "Processor %s is in state is %s."
            msg += "  This method only valid in state %s"
            raise Exception(msg % (
                self.__name,
                self._state_code_desc(self.__state),
                self._state_code_desc(expected_state)
                ))

        
    # -- Methods to be called before thread starts (NOT THREAD SAFE) ----------
    
    def df_create_input_port(self, input_name, prc_manger, conn_id):
        '''Inform the manager about another Processor connected to an input
        
        @param input_name: Name of the input on this processor
        @param prc_manger: EtlProcessorEventManager for connected processor
        '''
        self._setup_phase_method()  # Needed?
        raise NotImplementedError("TODO")

        # Validate input name
        assert(self.__inputs.has_key(input_name))
        assert(self.__input_ports.has_key(input_name))
        assert(not self.__conn_by_id.has_key(conn_id))

        # Save Connection Detail        
        conn = EtlInputConnection()
        
        conn.id = conn_id
        conn.status = self.CONN_CONNECTED
        conn.prc_name = prc_manger.prc_name
        conn.port_name = input_name
        conn.schema = self.__input_ports[input_name].schema
        
        self.__inputs[input_name].append(conn)
        self.__conn_by_id[conn_id] = conn
        
        
    def df_create_output_port(self, output_name, prc_manger, input_name, conn_id):
        '''Inform the manager about another Processor connected to an output
        
        @param output_name: Name of the output on this processor
        @param prc_manger: EtlProcessorEventManager for connected processor
        @param input_name: Name of input on other processor receiving records
        '''
        self._setup_phase_method()  # Needed?
        raise NotImplementedError("TODO")

        # Validate output name
        assert(self.__outputs.has_key(output_name))
        assert(self.__output_ports.has_key(output_name))
        assert(not self.__conn_by_id.has_key(conn_id))
        
        # Save Connection Detail
        conn = EtlOutputConnection()
        
        conn.conn_id = conn_id
        conn.status = self.CONN_CONNECTED
        conn.prc_manager = prc_manger
        conn.prc_name = prc_manger.prc_name
        conn.port_name = output_name
        conn.schema = self.__output_ports[output_name].schema
        conn.event_queue = prc_manger.get_event_queue()
        conn.record_queue = prc_manger.get_record_queue(input_name)

        self.__outputs[input_name].append(conn)
        self.__conn_by_id[conn_id] = conn


    def _lock_input_port(self, port_name):
        '''Lock an input port

        This is used to block processors trying to send records on this input
        port.  The intended use is to assist a processor in processing all
        input on a specific port, before recieving records on another input,
        without having to shelve a bunch of records.

        I believe that this can only safely be done during setup to avoid
        deadlock:
            1) Record received during RUINNNG
            2) Record being dispatched on port to be locked by external
               processor, but queue is full
                 a) External processor gets input lock
                 b) External processor starts to queue record, but call
                    is blocked because queue is full.
            3) This processor descides to lock the input and calls
               _lock_input_port()
            4) _lock_input_port() blocks while trying to get input lock
        '''
        self._startup_phase_method()
        self._input_ports[port_name].input_lock.aquire()


    def _unlock_input_port(self, port_name):
        '''Unlock an input port locked by _lock_input_port()

        Will allow connected external processors to start sending records
        on this port name.
        '''
        self._processing_phase_method()
        self._input_ports[port_name].input_lock.release()

    
    def start_processor(self):
        '''Begin processing records'''
        self._setup_phase_method()

        self._pr_start_children()
        self._pr_run_processor()


    # -- Processor execution --------------------------------------------------

    def _pr_run_processor(self):
        '''Perform the task of the processor'''
        self._setup_phase_method()

        # Do STARTUP Tasks
        self.__state = STARTUP_PHASE
        self.starting_processor()   # Hook for derived classes

        # Begin PROCESSING
        self.__state = RUNNING_PHASE
        self._extract_records()
        self._pr_event_loop()

        # Finished
        self.__state = FINISHED


    def _pr_event_loop(self):
        '''This is the primary loop that the processor runs while in RUNNING

        Though, if a processor doesn't revieve input, it probably won't spend
        much time here.  This is the glue between the if_* methods and the
        pr_* methods.
        '''
        
        # Receive events and dispatch to handler methods
        while self.waiting_on_more_input():

            event_type, event = self.__event_queue.get()
            
            if event_type == 'input_port_opened':
                self._pr_input_port_opened(event)

            else:
                raise Exception("Unknown event type: " + event_type)

            
            
    def waiting_on_more_input(self):
        '''Check to see if input ports are still open'''
        self._processing_phase_method()
        for input_name, input_port in self._input_ports:
            if input_port.has_open_connections():
                return True

        return not self.__event_queue.empty()   # Last check to see if there
                                                # are more events to process


    def extract_records(self):
        '''Hook for processor to extract/generate records
        
        These are records that are *not* created from processing input records,
        but rather are generated completely by the processor.  It is called
        before any input records are received if inputs are connected.
        
        If you need to generate records after all input records are processed,
        use the handle_input_disconnected() hook.
        '''
        pass


    # -- Port connection even handling ---------------------------------------

    def _if_input_port_opened(self, port_name, src_prc_name, src_prc, src_port):
        '''Notify this processor that one of it's input ports have been opened'''

        event = {
            'port_name': port_name,
            'src_prc_name': src_prc_name,
            'src_prc': src_prc,
            'src_port': src_port,
            }

        # This can be called during SETUP, STARTUP, or RUNNING!
        if self.__state in (self.SETUP_PHASE, self.STARTUP_PHASE):
            self._pr_input_port_opened(event)

        elif self.__state == self.RUNNING_PHASE:
            self.__event_queue.put('input_port_opened', event)

        else:
            raise Exception("Not a valid state for this call")


    def _pr_input_port_opened(self, event):
        raise NotImplementedError('TODO')


    # -- Incoming record handling --------------------------------------------

    def _if_recieve_input_record(self, input_port_name, record):
        event = {
            'input_port':   input_port_name,
            'record':       record,
        }

        if not self._input_ports.has_key(input_port_name):
            msg = "This processor does not have input named %s"
            raise IndexError(msg % (input_port_name))

        # Use read lock
        with self._input_ports[input_port_name].input_lock:
            self.__event_queue.put('recieve_input_record', event)


                            
    def _handle_input_record_event(self, event):
        '''Handle record recieved on input port'''
        msg = "Received message"
        if not self._validate_input_name(msg, event.input_name, event.conn_id):
            return False
        
        # Validate input state
        if self.__conn_by_id[event.conn_id].status == self.CONN_CLOSSED:
            msg = "Received message for a closed input %s" % (event.input_name)
            self.notify_error(msg)
            return False
        
        # If a record is held on that port, then ignore event
        input_name = event.input_name
        if self.__held_records[input_name] is not None:
            return False
            
        # Get record to be processed
        record = None
        try:
            record = self.__input_queues[input_name].get_nowait()
        except Empty:
            return False
        
        # Pass to processor
        dispatcher = self.dispatch_output_record
        action = self.prc.process_input_record(record, dispatcher)
        
        # Handle processor requested action
        if action is None:
            action = RecordConsumed()
        
        if action.code == 'record_consumed':
            return False
        elif action.code == 'hold_record':
            self.__held_records[input_name] = record
            return False
        elif action.code == 'get_next_record':
            return True
        else:
            msg = "Invalid post-record action code: %s"
            raise Exception(msg % (action.code))
        
        
    def _validate_input_name(self, context, input_name, conn_id):
        # Validate input name exists
        if not self.__inputs.has_key(input_name):
            msg = "%s on unknown input '%s'"
            self.notify_error(msg % (context, input_name))
            return False
            
        # Validate connection ID exists
        if not self.__conn_by_id.has_key(conn_id):
            msg = "%s on unknown connection '%s'"
            self.notify_error(msg % (context, conn_id))
            return False
            
        # Validate connection ID correct
        if self.__conn_by_id[conn_id].port_name != input_name:
            msg = "%s for input %s using connection ID %s"
            msg += " but, connection %s is port %s"
            msg = msg % (context, input_name, conn_id, conn_id,
                         self.__conn_by_id[conn_id].port_name)
            self.notify_error(msg)
            return False
        
        return True
          
          
    def _handle_disconnect_event(self, event):
        input_name = event.input_name
        conn_id = event.conn_id
        
        msg = "Received disconnect"
        if self._validate_input_name(msg, input_name, conn_id):
            
            # Close Connection
            self.__inputs[input_name].status = self.CONN_CLOSSED
            
            # Check to see if all connections to this input are clossed
            any_open = False
            for conn in self.__inputs[input_name]:
                if conn.status != self.CONN_CLOSSED:
                    any_open = True
                    
            if not any_open:
                dispatcher = self.dispatch_output_record
                self.prc.handle_input_disconnected(input_name, dispatcher)
    
    
    def dispatch_output_record(self, output_name, record):
        '''Called by EtlProcessor to send generated records out'''
        
        # Validate output name
        if not self.__output_ports.has_key(output_name):
            msg = "Output named '%s' does not exist.  " % (output_name)
            msg += "Use one of the following: "
            msg += ", ".join(self.__output_ports.keys())
            self.notify_dispatch_error(record, msg)
            return
            
        # Validate record matches output schema
        schema = self.__output_ports[output_name].schema
        errors = schema.check_record_struct(record)
        if len(errors) > 0:
            for error in errors:
                msg = "Record fails validation: " + error
                self.notify_dispatch_error(record, msg)
            return
        
        # Finish setting attributes on the record
        if not record.is_frozen:
            record.set_source(self.prc_name, output_name)
            record.freeze()
            
        # Send to connected processor managers
        for conn in self.__connected_outputs[output_name]:
            
            # Send Record
            queue = conn.dst_prc_manager.get_input_record_queue(conn.input_name)
            queue.put(record)
    
            # Send Event
            queue = conn.dst_prc_manager.get_event_queue()
            event = InputRecordRecieved(conn.input_name)
            queue.put(event)
            
    
    def notify_dispatch_error(self, record, error_msg):
        '''Record an error encountered with a generated record'''
        self.notify_error(record.create_msg("CANNOT DISPATCH MSG: " + error_msg))
        
        
    def notify_record_error(self, record, error_msg):
        '''Record an error encountered with a received record'''
        msg = "RECEIVCED INVALID MESSAGE: " + error_msg
        self.notify_error(record.create_msg(msg))
        
        
    def notify_error(self, error):
        msg ="Error encountered in event loop with processor %s: %s"
        msg = msg % (self.prc_name, error)
        print msg
                
    
    # -- Methods to be called from other threads (THREAD SAFE) ----------------
    
    def get_event_queue(self):
        return self.__event_queue
    
    
    def get_record_queue(self, input_name):
        return self.__input_queues[input_name]
    

    # -- Sub Processors ------------------------------------------------------


    def df_add_child_processor(self, child):
        '''Add a child processor.

        This processor will start in the SETUP state, and can be started by
        start_child_processor().
        '''


    # def df_connect_sib_processors(self)
