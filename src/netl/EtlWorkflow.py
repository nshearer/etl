'''
Created on Dec 27, 2012

@author: nshearer
'''
import os

from .EtlComponentRunner import EtlComponentRunner
from .EtlSession import EtlSession

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
        self.session.tracer.setup(path, overwrite, keep)


    def log_to_console(self, level=LOG_INFO):
        self.tracer.console_log_level = level


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


    def start(self):
        '''Begin the workflow execution'''

        # Attach context and tracer to each component
        for comp in self.components:
            comp.setup_etl(self.session)

        # Start each component
        for comp in self.components:
            runner = EtlComponentRunner(comp)
            self.__runners.append(runner)


    def wait(self):
        '''Wait until all components are finished'''
        while len(self.__runners) > 0:

            # Wait for runner
            runner = self.__runners[0]
            runner.join()

            # Pop runner
            runner = self.__runners.pop(0)
            runner.join() # Wait again.  Runner should be finished

