'''
Created on Dec 27, 2012

@author: nshearer
'''
from abc import ABCMeta, abstractmethod

class EtlProcessorDataPort(object):
    '''Specify a name for input or output record sets'''
    def __init__(self, name, schema):
        self.name = name
        self.schema = schema


class EtlProcessor(object):
    '''Takes 0 or more inputs and generates 0 or more outputs
    
    The EtlProcessor class is intended to be subclassed in order to create 
    the components of the ETL processor.  Each processor, then, performs one or
    more of the Extract, Transform, or Load functions.
    
    When subclassing, you must:
     1) Define list_input_ports() to describe which dataports the component
        has for receiving records
     2) Define list_output_ports() to describe which dataports the component has
        for sending generated or processed records to other processors.
     3) (optionally) define extract_records() to extract records from external
         sources and output them for use by other processors
           -) Call dispatch_output() to send generated records out
     4) (optionally) Define process_input_record() to consume incoming records
           -) Call dispatch_output() to send processed records out
     5) Define handle_input_disconnected() to respond to a processor that has
        disconnected from an input.  All processors must disconnect by calling
        output_is_finished() when no more records will be generated for that
        output.

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


    '''
    __metaclass__ = ABCMeta
    
    SETUP_PHASE = 1
    STARTUP_PHASE = 2
    RUNNING_PHASE = 3
    PAUSED = 4
    FINISHED = 5

    def __init__(self):
        self.data_dir_path = None
        self.tmp_dir_path = None
        self.__state = self.SETUP_PHASE
    
    
    # -- State Checking ------------------------------------------------------

    def _setup_phase_method(self):
        '''Checks that the method call is happening during setup'''
        if self.__state != self.SETUP_PHASE:
            raise Exception("Cannot call as phase is not SETUP")

    def _assert_in_startup_phase(self):
        pass

    def _assert_in_running_phase(self):
        pass

    def _assert_finished(self):
        pass


    @abstractmethod
    def list_input_ports(self):
        '''List datasets to be consumed by this processor.
        
        @return list of EtlProcessorDataPort objects
        '''
        return None
    
    
    @abstractmethod
    def list_output_ports(self):
        '''List the datasets produced by this processor
        
        @return list of EtlProcessorDataPort objects
        '''
        return None
        
    
    def list_input_names(self):
        '''List just the names of the input data sets'''
        inputs = self.list_input_ports()
        if inputs is not None:
            return [port.name for port in inputs]
        return list()
        
        
    def list_output_names(self):
        '''List just the names of the input data sets'''
        outputs = self.list_output_ports()
        if outputs is not None:
            return [port.name for port in outputs]
        return list()
    
    
    def extract_records(self):
        '''Hook for processor to extract/generate records
        
        These are records that are *not* created from processing input records,
        but rather are generated completely by the processor.  It is called
        before any input records are received if inputs are connected.
        
        If you need to generate records after all input records are processed,
        use the handle_input_disconnected() hook.
        '''
        pass
    
#         
#         Dynamically calls 'gen_<name>_output' method
#         
#         @param name: Name of the output to generate
#         @param inputs: Dictionary of connected input datasets
#         @param record_set: Container to populate with records
#         '''
#         # Get method to generate records
#         gen_name = 'gen_%s_output' % (name)
#         try:
#             gen = getattr(self, gen_name)
#         except AttributeError:
#             msg = 'Missing output generator: %s' % (gen_name)
#             raise Exception(msg)
#         
#         # Call method to generate records
#         gen(inputs, record_set)
#         
    #def gen_<name>_output(self, inputs, output_set):
    #    for record_set in inputs['main_input_name']:
    #        for record in record_set.all_records():
    #            output_set.add_record({
    #                'Field':   record['field_value'],
    #                }, index=field_value)
    
        