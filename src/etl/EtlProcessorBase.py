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

        (outside thread)            +                              
                                    |                              
                                    |                              
                                    |                              
               if_[name](args) +----------------> Queue            
                                    |                              
                                    |               +              
        +---------------------------+               |              
                                                    |              
                                                    v              
               pr_[name](args) <----------------+ pr_process_events


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


    def _asert_phase_is(self, expected_state):
        if self.__state != expected_state:
            msg = "Processor %s is in state is %s."
            msg += "  This method only valid in state %s"
            raise Exception(msg % (
                self.__name,
                self._state_code_desc(self.__state),
                self._state_code_desc(expected_state)
                ))



    
    # # The states of input connections
    # CONN_CONNECTED = 0     # Initial state
    # CONN_CLOSSED   = 1     # State after PrcDisconnectedEvent
    

             
#         # 
#         for conn in output_connections:
#             if not self.__connected_outputs.has_key(conn.output_name):
#                 self.__connected_outputs[conn.output_name] = list()
#             self.__connected_outputs[conn.output_name].append(conn)

        
    # -- Methods to be called before thread starts (NOT THREAD SAFE) ----------
    
    def create_input_port(self, input_name, prc_manger, conn_id):
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
        
        
    def create_output_port(self, output_name, prc_manger, input_name, conn_id):
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

    
    @abstractmethod
    def start_processor(self):
        '''Begine processing records'''
        #self._setup_phase_method()



    # -- Methods to be called from this thread (NOT THREAD SAFE) --------------
        




    def run_event_loop(self):
        '''This is the "main" loop of this thread'''
        
        # Let processor extract records
        self.prc.extract_records(self.dispatch_output_record)
        
        # Receive events from other processors
        while self.waiting_on_more_input():
            event = self.__event_queue.get()
             
            if event.type == 'input_record':
                get_next = True
                while get_next:
                    get_next = self._handle_input_record_event(event)
                    
            elif event.type == 'input_disconnected':
                self._handle_disconnect_event(event)
                    
            else:
                raise Exception("Unknown event type: " + event.type)
            
            
    def waiting_on_more_input(self):
        for input_name in self.__inputs:
            if self.__inputs[input_name].status != self.CONN_CLOSSED:
                return True
        return False
                    
                            
    def _handle_input_record_event(self, event):
        '''Handle an incoming record
        
        @return: True if GetNextInputRecord returned
        '''
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
    
    
