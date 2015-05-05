from Queue import Queue, Empty

from abc import ABCMeta, abstractmethod

import ports
from exceptions import EtlBuildError, InvalidProcessorName


class EtlEvent:
    '''Simple collection of attributes for event queue.
    
    See http://code.activestate.com/recipes/52308-the-simple-but-handy-collector-of-a-bunch-of-named/
    '''
    
    def __init__(self, event_type, **kwds):
        self.event_type = event_type
        self.__dict__.update(kwds)


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


                    +-------+     run_processor()   +---------+
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

        self._input_queue = Queue(maxsize=self.MAX_EVENT_Q_SIZE)
        
        self._input_ports = ports.InputPortCollection()
        self._output_ports = ports.OutputPortCollection()
        
        self._sub_processors = dict()


    @property
    def processor_name(self):
        return self.__name
    


    # -- State Checking ------------------------------------------------------

    def _setup_phase_method(self):
        '''Checks that the method call is happening during setup'''
        self._asert_phase_is(self.SETUP_PHASE)


    def _startup_phase_method(self):
        '''Checks that the method call is happening during startup'''
        self._asert_phase_is(self.STARTUP_PHASE)


    def _processing_phase_method(self):
        '''Checks that the method call is happening during processing'''
        self._asert_phase_is(self.RUNNING_PHASE)


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
    
    def df_create_input_port(self, name):
        '''Inform the manager about another Processor connected to an input
        
        @param input_name: Name of the input on this processor
        @param prc_manger: EtlProcessorEventManager for connected processor
        '''
        self._setup_phase_method()  # Needed?
        self._input_ports.create_port(name)

        
    def df_create_output_port(self, name):
        '''Inform the manager about another Processor connected to an output
        
        @param output_name: Name of the output on this processor
        @param prc_manger: EtlProcessorEventManager for connected processor
        @param input_name: Name of input on other processor receiving records
        '''
        self._setup_phase_method()  # Needed?
        self._output_ports.create_port(name)


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
        self._setup_phase_method()
        self._input_ports[port_name].input_lock.acquire()


    def _unlock_input_port(self, port_name):
        '''Unlock an input port locked by _lock_input_port()

        Will allow connected external processors to start sending records
        on this port name.
        '''
        self._processing_phase_method()
        self._input_ports[port_name].input_lock.release()
    
    
    # -- Processor execution --------------------------------------------------

    def _boot_processor(self):
        '''Perform startup tasks for this processor'''
        self._setup_phase_method()  # We can only start if we're in SETUP

        # Do STARTUP Tasks
        self.__state = self.STARTUP_PHASE
        print "[%s] STARTUP" % (self.processor_name)
        self.starting_processor()   # Hook for derived classes

        # Start child processors
        print "[%s] Starting children" % (self.processor_name)
        for subproc in self._sub_processors.values():
            # Sub processors shouldn't be generators!
            subproc._boot_processor()
        print "[%s] All children started" % (self.processor_name)


    def _set_running(self):
        print "[%s] RUNNING" % (self.processor_name)
        self.__state = self.RUNNING_PHASE
        

    def _pr_process_records(self):
        '''Perform the task of the processor
        
        Note that this method is a generator.  Super classes can yield back
        records received.
        '''
        self._processing_phase_method()

        # Extract Records
        print "[%s] extract_records()" % (self.processor_name)
        self.extract_records()
        print "[%s] extract_records() finished" % (self.processor_name)
        
        # Process incoming records
        print "[%s] _pr_event_loop()" % (self.processor_name)
        for yeild_record in self._pr_event_loop():
            yield yeild_record  # Top level workflow can yield records
        print "[%s] _pr_event_loop() finished" % (self.processor_name)

        # Finished
        print "[%s] FINISHED" % (self.processor_name)
        self.__state = self.FINISHED


    def _pr_event_loop(self):
        '''This is the primary loop that the processor runs while in RUNNING

        Though, if a processor doesn't revieve input, it probably won't spend
        much time here.  This is the glue between the if_* methods and the
        pr_* methods.
        '''
        
        # Receive events and dispatch to handler methods
        while self.waiting_on_more_input():
            yield None  # TODO

            event = self._input_queue.get()
            
            if event.event_type == 'input_port_opened':
                self._pr_input_port_opened(event)
                
            elif event.event_type == 'recieve_input_record':
                self._pr_recieve_input_record(event)

            else:
                raise Exception("Unknown event type: " + event.event_type)

            
            
    def waiting_on_more_input(self):
        '''Check to see if input ports are still open'''
        self._processing_phase_method()
        for input_port in self._input_ports.values():
            if input_port.is_connected:
                return True

        return not self._input_queue.empty()   # Last check to see if there
                                               # are more events to process


    # -- Processor execution hooks --------------------------------------------
    
    def starting_processor(self):
        '''Hook for child class to do something while processor is stopping'''
        pass
    
    
    def extract_records(self):
        '''Hook for child classes to extract records from an external source'''
        pass


    # -- Port connection event handling ---------------------------------------

    def _if_input_port_opened(self, port_name, src_prc_name, src_prc_port):
        '''Notify this processor that one of it's input ports have been opened
        
        @param port_name: Name of input port that has been connected to
        @param src_prc_name: Name of source processor sending records to this
            processor's input
        @param src_prd_port: Name of output port on source processor that was
            connected to this processors input port
        '''

        event = EtlEvent(
            event_type      = 'input_port_opened',
            port_name       = port_name,
            src_prc_name    = src_prc_name,
            src_prc_port    = src_prc_port)

        # This can be called during SETUP, STARTUP, or RUNNING!
        if self.__state in (self.SETUP_PHASE, self.STARTUP_PHASE):
            self._pr_input_port_opened(event)

        elif self.__state == self.RUNNING_PHASE:
            self.__event_queue.put(event)

        else:
            raise Exception("Not a valid state for this call")


    def _pr_input_port_opened(self, event):
        self._input_ports[event.port_name].connected_by(event.src_prc_name,
                                                        event.src_prc_port)


    # -- Record output -------------------------------------------------------
    
    def pr_dispatch_output(self, output_port_name, record):
        '''Send a processed record out of the named output port
        
        This method is how records are sent to processors that have connected
        to this processors output port.
        
        Before being sent, the record is frozen to make it immutable.  This
        ensures that the record object can be passed to multiple processors
        without one changing the data and effecting the other.
        '''
        record.freeze()
        conns = self._output_ports[output_port_name].get_connected_prcs()
        for prc, prc_input_port_name in conns:
            prc._if_recieve_input_record(
                prc_name = self.processor_name,
                output_port = output_port_name,
                input_port = prc_input_port_name,
                rec = record)



    # -- Incoming record handling --------------------------------------------

    def _if_recieve_input_record(self, prc_name, output_port, input_port, rec):
        '''Receive a record from another processor
        
        It is assumed that the source processor has "connected" one of it's
        output ports to this processor's input port.
        
        @param prc_name: Source processor name
        @param output_port: Name of output port on source processor
        @param input_port: Name of the input port on this processor record was
            sent to.
        @param rec: The EtlRecord begin sent.
        '''
        event = EtlEvent(
            event_type = 'recieve_input_record',
            source_prc =    prc_name,
            output_port =   output_port,
            input_port =    input_port,
            record =        rec,
        )

        # Use input port lock
        with self._input_ports[input_port].input_lock:
            self._input_queue.put(event)


                            
    def _pr_recieve_input_record(self, event):
        '''Handle record received on input port'''
        
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
    
    
#     def dispatch_output_record(self, output_name, record):
#         '''Called by EtlProcessor to send generated records out'''
#         
#         # Validate output name
#         if not self.__output_ports.has_key(output_name):
#             msg = "Output named '%s' does not exist.  " % (output_name)
#             msg += "Use one of the following: "
#             msg += ", ".join(self.__output_ports.keys())
#             self.notify_dispatch_error(record, msg)
#             return
#             
#         # Validate record matches output schema
#         schema = self.__output_ports[output_name].schema
#         errors = schema.check_record_struct(record)
#         if len(errors) > 0:
#             for error in errors:
#                 msg = "Record fails validation: " + error
#                 self.notify_dispatch_error(record, msg)
#             return
#         
#         # Finish setting attributes on the record
#         if not record.is_frozen:
#             record.set_source(self.prc_name, output_name)
#             record.freeze()
#             
#         # Send to connected processor managers
#         for conn in self.__connected_outputs[output_name]:
#             
#             # Send Record
#             queue = conn.dst_prc_manager.get_input_record_queue(conn.input_name)
#             queue.put(record)
#     
#             # Send Event
#             queue = conn.dst_prc_manager.get_event_queue()
#             event = InputRecordRecieved(conn.input_name)
#             queue.put(event)
            
    
#     def notify_dispatch_error(self, record, error_msg):
#         '''Record an error encountered with a generated record'''
#         self.notify_error(record.create_msg("CANNOT DISPATCH MSG: " + error_msg))
#         
#         
#     def notify_record_error(self, record, error_msg):
#         '''Record an error encountered with a received record'''
#         msg = "RECEIVCED INVALID MESSAGE: " + error_msg
#         self.notify_error(record.create_msg(msg))
#         
#         
#     def notify_error(self, error):
#         msg ="Error encountered in event loop with processor %s: %s"
#         msg = msg % (self.prc_name, error)
#         print msg
#     

    # -- Sub Processors ------------------------------------------------------


    def df_add_child_processor(self, child):
        '''Add a child processor.

        This processor will start in the SETUP state, and can be started by
        start_child_processor().
        '''
        name = child.processor_name

        if self._sub_processors.has_key(name):
            raise IndexError("Duplicate processor name: %s" % (name))
        self._sub_processors[name] = child


    def get_child_prc(self, name):
        if not self._sub_processors.has_key(name):
            raise InvalidProcessorName(name, self._sub_processors.keys())
        return self._sub_processors[name]
    
    
    # -- Connecting processors ------------------------------------------------


    def df_connect_children(self, source_prc, source_port, dst_prc, dst_port):
        '''Connect the output of one child processor to the input of another
        
        @param source_prc: Name of the source child processor
        @param source_port: Name of the output port on the source processor
        @param dst_prc: Name of the destination child processor
        @param dst_port: Name of the input port on the destination processor
        '''
        source_prc = self.get_child_prc(source_prc)
        dst_prc = self.get_child_prc(dst_prc)
        
        source_prc._df_connect_to(source_port, dst_prc, dst_port)
        
        
    def _df_connect_to(self, source_port, dst_prc, dst_port):
        '''Connect this processor's output to another processors input
        
        @param source_port: Name of the output port for this processor
        @param dst_prc: Processor object to connect to
        @param dst_port: Name of the input port on the destination processor
        '''
        self._output_ports[source_port].connect_to(dst_prc, dst_port)
        dst_prc._if_input_port_opened(
            port_name=dst_port,
            src_prc_name=self.processor_name,
            src_prc_port=source_port)
        

#        if input_name is None:
#            input_name = output_name
#            
#        connect_desc = "%s.%s -> %s.%s"
#        connect_desc = connect_desc % (from_prc_name, output_name,
#                                       to_prc_name, input_name)
#        
#        # Sanity Checks
#        if not self.__processors.has_key(from_prc_name):
#            raise self._invalid_prc_name(from_prc_name,
#                                         "Building connection " + connect_desc)
#        from_prc = self.__processors[from_prc_name]
#
#        output_info = None
#        for p_output in from_prc.list_outputs():
#            if p_output.processor_name == output_name:
#                output_info = p_output
#                break
#        if output_info is None:
#            raise self._invalid_dataport_name(
#                direction = 'output',
#                prc_name = from_prc_name,
#                port_name = output_name,
#                context = "Building connection " + connect_desc)
#            
#        
#        if not self.__processors.has_key(to_prc_name):
#            raise self._invalid_prc_name(to_prc_name,
#                                         "Building connection " + connect_desc)
#        to_prc = self.__processors[to_prc_name]
#        
#        input_info = None
#        for p_input in to_prc.list_inputs():
#            if p_input.processor_name == input_name:
#                input_info = p_input
#                break
#        if input_info is None:
#            raise self._invalid_dataport_name(
#                direction = 'input',
#                prc_name = to_prc_name,
#                port_name = input_name,
#                context = "Building connection " + connect_desc)
#        
#        # Build connection definition
#        conn = WorkflowDataPath()
#        conn.src_prc_name = from_prc_name
#        conn.src_prc = from_prc
#        conn.output_name = output_name
#        conn.output_schema = output_info.schema
#        conn.dst_prc_name = to_prc_name 
#        conn.dst_prc = to_prc
#        conn.input_name = input_name
#        conn.input_schema = input_info.schema
#        
#        # Add connection definition
#        self.__connections[to_prc_name][input_name].append(conn)


#     # -- Exception Builders ---------------------------------------------------
#         
#     def _invalid_prc_name(self, prc_name, context):
#         '''Create an InvalidProcessorName exception object
#         
#         @param prc_name: The processor name that does not exist
#         @param context: Where the invalid name was used at
#         '''
#         msg = "Invalid processor name '%s' referenced by %s"
#         msg = msg % (prc_name, context)
#         
#         return InvalidProcessorName(
#             prc_name = None,
#             prc_class_name = None,
#             error_msg = msg,
#             possible_values = self.__processors.keys())
#     
#     
#     def _invalid_dataport_name(self, direction, prc_name, port_name, context):
#         '''Create an InvalidProcessorName exception object
#         
#         @param direction: 'input' or 'output'
#         @param prc_name: The processor being referenced with the data port
#         @param port_name: The non-existant dataport that's being referenced
#         @param context: Where the invalid name was used at
#         '''
#         msg = "Processor %s (%s) does not have an %s port named '%s'"
#         msg += " referenced by %s"
#         msg = msg % (prc_name,
#                      self.__processors[prc_name].__class__.__name__,
#                      direction,
#                      port_name,
#                      context)
#         
#         possible = None
#         if direction == 'input':
#             possible = self.__processors[prc_name].list_input_names()
#         elif direction == 'output':
#             possible = self.__processors[prc_name].list_output_names()
#         else:
#             raise ValueError("Invalid direction: '%s'" % (direction))
#             
#         
#         return InvalidDataPortName(
#             prc_name = prc_name,
#             prc_class_name = self.__processors[prc_name].__class__.__name__,
#             error_msg = msg,
#             possible_values = possible)


    # -- Debug ----------------------------------------------------------------
    
    def debug_etl_graph_as_dot(self):
        '''Describe this processor and all children as a graphviz graph'''
        src = list()
        
        src.append('digraph %s {' % (self.__class__.__name__))
        
        src.append('rankdir = "LR";')
        src.append('node [shape=Mrecord];')
        
        # Self
        src.append(self._debug_processor_as_dot(self))
        
        # Children
        for prc in self._sub_processors.values():
            src.append(self._debug_processor_as_dot(prc))
            for output_port_name in prc._output_ports.keys():
                conns = prc._output_ports[output_port_name].list_connections()
                for target_prc_name, target_port_name in conns:
                    src.append('%s:o_%s -> %s:i_%s;' % (
                        prc.processor_name,
                        output_port_name,
                        target_prc_name,
                        target_port_name))
            
        
        src.append('}')
        
        return "\n".join(src)
        
    def _debug_processor_as_dot(self, prc):
        src = list()
        
        input_ports = sorted(prc._input_ports.keys())
        output_ports = sorted(prc._output_ports.keys())
        
        input_port_defs = ['<i_%s> %s' % (name, name) for name in input_ports]
        output_port_defs = ['<o_%s> %s' % (name, name) for name in output_ports]
        
        src.append('%s [label="{ %s\\n\'%s\' | { { %s } | { %s } } }"];' % (prc.processor_name,
                                              prc.__class__.__name__,
                                              prc.processor_name,
                                              " | ".join(input_port_defs),
                                              " | ".join(output_port_defs),
                                              ))
        
        return "\n".join(src)
    
        
        