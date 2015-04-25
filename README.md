Nate's ETL Library
==================

Python ETL Library for facilitating data transformations.

EtlProcessorBase
----------------

Base class for EtlProcessor

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

@see EtlProcessor


EtlProcessor
------------

Takes 0 or more inputs and generates 0 or more outputs

See EtlProcessorBase for additional detail

The EtlProcessor class is intended to be subclassed in order to create 
the components of the ETL processor.  Each processor, then, performs one or
more of the Extract, Transform, or Load functions.

TODO: Update
When subclassing, you must:
 1) Define list_input_ports() to describe which dataports the component
    has for receiving records
 2) Define list_output_ports() to describe which dataports the component has
    for sending generated or processed records to other processors.
 3) (optionally) define starting_processor() to perform any startup tasks
 4) (optionally) define extract_records() to extract records from external
     sources and output them for use by other processors
       -) Call dispatch_output() to send generated records out
 5) (optionally) define methods to process records sent to this component's 
    input ports.


     process_input_record() to consume incoming records
       -) Call dispatch_output() to send processed records out
 5) Define handle_input_disconnected() to respond to a processor that has
    disconnected from an input.  All processors must disconnect by calling
    output_is_finished() when no more records will be generated for that
    output.




