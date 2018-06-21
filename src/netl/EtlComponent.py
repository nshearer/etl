'''
Created on Dec 27, 2012

@author: nshearer
'''
from abc import ABC, abstractmethod

from .constants import *

class EtlComponent(ABC):
    '''
    Takes 0 or more inputs and generates 0 or more outputs

    The EtlComponent class is intended to be subclassed in order to create
    the components of the ETL workflow.  Each processor, then, performs one or
    more of the Extract, Transform, or Load functions in it's own process.
    
    When subclassing, you must:

    1)  Define any input channels for your component to receive records as class
        level properties using EtlInput

    2) Define any output channels for your component to send out processed records
       as class level properties using EtlOutput

    3) Implement run() to do the work of the component

    Example
    -------

        form netl import EtlComponent, EtlInput, EtlOutput, EtlConnectionClosed

        class MyCustomTransform(EtlComponent):

            # -- Component Connections -------------------------------------
            equations = EtlInput()
            answers = EtlOutput()
            # --------------------------------------------------------------


            def run(self):
                while True:
                    try:
                        for eq in self.equations.get():
                            answer = do_soemthing(eq)
                            self.ansers.output(answer)
                    except EtlConnectionClosed:
                        break


    '''


    def __init__(self):
        self.context = None
        self.trace = None


    @property
    def name(self):
        return self.__class__.__name__


    @property
    def outputs(self):
        '''All output ports (name, port)'''
        for name in dir(self):
            if name[0] != '_':
                try:
                    attr = getattr(self, name)
                    if attr.is_etl_output_port:
                        yield name, attr
                except AttributeError:
                    pass


    def setup(self, context, trace):
        '''Called by workflow just prior to starting'''
        self.context = context
        self.trace = trace

        # Setup output connections to tag outbound records
        for name, port in self.outputs:
            port._src_component_name = self.name
            port._output_name = name


    def _execute(self):
        '''Top level method in running thread / process'''
        self._startup()
        self.run()
        self._finish()


    def _startup(self):
        self.log("Starting %s" % (self.name), LOG_DEBUG)

    def _finish(self):
        self.log("%s Finishes" % (self.name), LOG_DEBUG)


    @property
    def any_input(self):
        '''Special input to allow component to receive input records from any input channel'''
        # TODO: any_input
        raise NotImplementedError()

    @property
    def is_etl_component(self):
        return True


    # -- Helper / Proxy methods -------------------------------------------------------

    def log(self, message, level=LOG_INFO):
        try:
            message = '[%s] %s' % ((self.name.upper()[:4] + '    ')[:4], message)
            self.trace.log(message, level)
        except AttributeError:
            raise Exception(".trace missing.  Is component stated yet?  Check EtlWorkflow.start()")
