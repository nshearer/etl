Nate's ETL Library
==================

Python ETL Library for facilitating data transformations.

Project Goals
-------------

 1. Quick to implement ETL method based on flowing data between components
 2. Useful debugging tools to help visualize and troubleshoot your data flow

Summary
-------

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

[include:EtlWorkflow]

[include:EtlWorkflow:methods]


Processors
==========

EtlProcessorBase
----------------

[include:EtlProcessorBase]

EtlProcessor
------------

[include:EtlProcessor]
