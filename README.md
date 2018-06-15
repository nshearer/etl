Nate's ETL Library
==================

Python ETL Library for facilitating data transformations.

I am frequently trying to implement ETL workflows in Python that follow the pattern
of multiple components doing one thing well and passing records bewtween each other.
However, doing this well often proves challenging.  This tool aims to solve this
by being lightweight and providing the features:

 - Each component in the ETL graph can be implemented as a single object
 - Separate components will run in separate processes, taking advantage of threading
 - A context variable is supported to create a standard method of loading parameters,
   credentials.
 - The concept of immutable records being passed between processors hepls ensure that
   records can be processed in parallel.  Achieved by passing pickled objects.
 - No schema checking is enforced by the tool.
 - A class is provided to quickly and efficiently buffer records to disk when a component
   needs to wait until all prior records are received.

Aditionally, debugging features are provided to help with troubleshooting workflows
that span multiple threads.

 - A seperate logging thread is provided to ensure clean logs
 - The entire record path can be traced and analyzed.
 - Generation of a graphical representation of the Workflow is possible with GraphViz
 - Live monitoring of the progress of the ETL can be seen.
 - A system is in place to link derived records from the records used to produce them,
   providing the ability to trace information flowing through the ETL graph.


Workflow
========

Encapsulates an ETL workflow

This class is used to organize all of the components in the workflow.
It is itself a processor in the ETL Workflow, refered to as the root
processor.  Unlike child processors based on EtlProcessor, though,
this root processor runs in the same thread as the invoking code.

Definition of ETL:
------------------
from [Wikipedia](http://en.wikipedia.org/wiki/Extract,_transform,_load)

In computing, Extract, Transform and Load (ETL) refers to a process in
database usage and especially in data warehousing that:

Extracts data from homogeneous or heterogeneous data sources Transforms the
data for storing it in proper format or structure for querying and analysis
purpose Loads it into the final target (database, more specifically,
operational data store, data mart, or data warehouse) Usually all the three
phases execute in parallel since the data extraction takes time, so while
the data is being pulled another transformation process executes, processing
the already received data and prepares the data for loading and as soon as
there is some data ready to be loaded into the target, the data loading
kicks off without waiting for the completion of the previous phases.

ETL systems commonly integrate data from multiple applications(systems),
typically developed and supported by different vendors or hosted on separate
computer hardware. The disparate systems containing the original data are
frequently managed and operated by different employees. For example a cost
accounting system may combine data from payroll, sales and purchasing.


Creating an ETL Process
-----------------------

In order to define an ETL process, the developer is encouraged to
subclass EtlWorkflow and then define and connect the processors.
This class does not need to be subclassed to create an ETL process,
though, as you can just instantiate it and call the methods.

 1) Define your processors by subclassing EtlProcessor, or using the
    common processors under netl.common

 2) Call add_processor() to add your processors to the Workflow

 3) Call connect() to connect the output ports of processors to the
    input ports of other processors

 4) Call assign_processor_output() to connect the output ports of
    processors to an input port of this Workflow object.  This allows
    you to define a path for records to exit the ETL workflow and
    be returned to the calling code.  When you call the workflow.
    execute() method to run the ETL process, and records dispatched
    on the specified port will be yielded back to the calling function.

  5) Call exectue() - Run the workflow to generate the desired output. 


Methods of EtlWorkflow
------------------------------------



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
                    longer receive or dispatch records.


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
of the method.  This serves both to inform when the method can be
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
   using that method's normal signature.

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


EtlProcessor
------------

Takes 0 or more inputs and generates 0 or more outputs

See EtlProcessorBase for additional detail

The EtlProcessor class is intended to be subclassed in order to create 
the components of the ETL processor.  Each processor, then, performs one or
more of the Extract, Transform, or Load functions in it's own thread.

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

