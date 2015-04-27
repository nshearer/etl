Nate's ETL Library
==================

Python ETL Library for facilitating data transformations.



Processors
==========

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

@see EtlProcessor


EtlProcessor
------------

Takes 0 or more inputs and generates 0 or more outputs

See EtlProcessorBase for additional detail

The EtlProcessor class is intended to be subclassed in order to create 
the components of the ETL processor.  Each processor, then, performs one or
more of the Extract, Transform, or Load functions.

When subclassing, you must:

1)  In your __init__():
    a) Call the super init()
    b) Call df_create_input_port() to define input ports
    c) Call df_create_output_port() to define output ports

2)  (optionally) define starting_processor() to perform any startup tasks

3)  (optionally) define extract_records() to extract records from external
     sources and output them for use by other processors

       - Call dispatch_output() to send generated records out

4)  (optionally) define methods to process records sent to this component's 
    input ports.

      - Define pr_<name>_input_record() to process records recieved on
        port named <name>.
      - Define pr_any_input_record() to consume incoming records not handled
        by a method setup for a speific port name.

      - Call pr_dispatch_output() to send processed records out
      - Call pr_hold_record() to stash a record for processing later
      - Call pr_unhold_records() to retrieve previously held records
      - Call pr_output_finished() to signal that no more output will
        be sent on the named port.  When all output ports are clossed,
        then the processing loop will exit.

5)  Define pr_handle_input_clossed() to perform any final processing when
    an input port is clossed.  All of the methods available to the input
    handling methods are available here.


EtlSchema
=========

Describes the structure of a record

The purpose of the schema is to assist the ETL logic with handling the
fields of records when the ETL library does not know the structure
of the records that will be used.  I've gone back and forth as to
whether to even require schemas, and whether to lock down input and
output ports to schemas.  Managing schemas, in my experience, can become
tedious and self-serving.

I've decided to keep schemas though to assist with:

    - Freezing records from changes
    - Serializeing records to disk
    - Debuging/Describing records to the user

I will not provide a mechanism to lock/check schemas on processor ports,
though, and I don't see a great advantage to requiring this.  This leaves
a processor to be flexible in recieving multiple record types if desired,
and leaves it up to the developer to ensure that the required fields for a
given processor are present.  I feel this supports the common Python
practice of Duck Typing.

Each record, however, does need to have an associated schema.
