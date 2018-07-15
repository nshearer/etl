'''
Created on Dec 27, 2012

@author: nshearer
'''
import logging

from .EtlComponentRunner import EtlComponentRunner
from .EtlSession import EtlSession
from .tracedb import TraceDB, TraceETLStateChange

from .constants import LOG_INFO

class EtlWorkflow:
    '''
    Defines the ETL components and record passing paths for

    This class is used to organize all of the components in the workflow.

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


    TODO: Add steps

    TODO: Add example

    '''

    def __init__(self):

        self.session = EtlSession()

        self.__runners = list()


    def trace_to(self, path, overwrite=False, keep=True):
        '''
        Sets the path where trace files will be written to
        '''
        self.session.tracer.setup_tracer(path, overwrite, keep)


    @property
    def components(self):
        '''All the components of the workflow'''
        for attr in dir(self):
            if attr[0] != '_':
                try:
                    attr = getattr(self, attr)
                    if attr.is_etl_component:
                        yield attr
                except AttributeError:
                    pass


    def add_resource(self, resource):
        '''
        Add a shared resource that can be used by components

        :param resource: EtlResource object
        '''
        self.session.resources.add(resource)


    def std_logging(self, level=logging.INFO):
        '''Setup Standard logging'''
        logger = self.session.get_logger()

        logger.setLevel(level)
        formatter = logging.Formatter('%(levelname)-8s %(name)s - %(message)s')
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        logger.addHandler(console)

        return logger


    def start(self):
        '''Begin the workflow execution'''

        # Update freezer
        self.session.freezer.mark_started()

        # Start the tracer
        if self.session.tracer.tracing_enabled:
            self.session.tracer.start()

        # Attach ETL session to each component
        for comp in self.components:
            comp.setup_etl(self.session)

        # Start each component
        for comp in self.components:
            runner = EtlComponentRunner(comp)
            self.__runners.append(runner)

        # Log that we started
        self.session.tracer.trace(TraceETLStateChange(
            state = TraceDB.RUNNING_STATE
        ))


    def wait(self):
        '''Wait until all components are finished'''
        while len(self.__runners) > 0:

            # Wait for runner
            runner = self.__runners[0]
            runner.join()

            # Pop runner
            runner = self.__runners.pop(0)
            runner.join() # Wait again.  Runner should be finished

        # Log that we finshed
        self.session.tracer.trace(TraceETLStateChange(
            state = TraceDB.FINISHED_STATE
        ))


    def finish(self):
        '''Shutdown the workflow (will wait until all components have finished)'''
        self.wait()
        if self.session.tracer.tracing_running:
            self.session.tracer.stop_tracer()

        # Log that we finshed
        self.session.tracer.trace(TraceETLStateChange(
            state = TraceDB.FINISHED_STATE
        ))


    @property
    def logger(self):
        try:
            return self.__logger
        except AttributeError:
            self.__logger = self.session.get_logger('WF')
            return self.__logger
