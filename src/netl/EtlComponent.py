'''
Created on Dec 27, 2012

@author: nshearer
'''

from .constants import *
from .EtlSession import EtlObject
from .serial import EtlSerial

from .tracedb import ComponentTrace, TraceNewComponent, TraceComponentStateChange
from .tracedb import TraceComponentPortExists, TraceConnection

class EtlComponent(EtlObject):
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


    @property
    def name(self):
        return self.__class__.__name__


    @property
    def component_id(self):
        '''Unique ID to identify this component'''
        try:
            return self.__component_id
        except AttributeError:
            self.__component_id = int(str(EtlSerial()))
            return self.__component_id


    @property
    def logger(self):
        try:
            return self.__logger
        except AttributeError:
            self.__logger = self.session.get_logger(self.name)
            return self.__logger

    def mask_port_attrs(self, *attrs):
        '''
        Prevent ports() attribute auto-discovery logic from access non-port attributes

        ports() basically iterates over all of the non-private attribuets of this objects
        and checks attr.is_etl_port to determine if the attribute is a port.  This can
        cause effects on @property methods that run logic when accessed.

        Should this cause an issue, call mask_port_attrs with a list of attributes to filter
        out of the list of candidate attributes and prevent ports() from accessing it.
        '''
        try:
            self.__masked_port_attrs.extend(attrs)
        except AttributeError:
            self.__masked_port_attrs = list(attrs)


    @property
    def ports(self):
        '''
        All ports (name, port)

        Introspects self to find port attributes
        '''

        # Mask out a set of attributes to not attempt to query to see if it's a port
        mask_out = set(('ports', 'outputs', 'inputs', 'setup_etl', 'is_etl_component',
                        'name', 'mask_port_attrs', 'logger'))
        try:
            for attr in self.__masked_port_attrs:
                mask_out.add(attr)
        except AttributeError:
            pass

        # Look over all public attributes
        for name in filter(lambda n: n[0] != '_' and n not in mask_out, dir(self)):

            # Note: This in effect calls all @property methods.
            #       Need to be a bit careful.  See mask_port_attrs()

            try:
                attr = getattr(self, name)
                if attr.is_etl_port:
                    yield name, attr
            except AttributeError:
                pass


    @property
    def outputs(self):
        '''All output ports (name, port)'''
        return filter(lambda p: p.is_etl_output_port, [t[1] for t in self.ports])


    @property
    def inputs(self):
        '''All output ports (name, port)'''
        return filter(lambda p: p.is_etl_input_port, [t[1] for t in self.ports])


    def setup_etl(self, session): # TODO: Should probably rename setup_etl() to bootstrap_etl()

        super(EtlComponent, self).setup_etl(session)

        # Trace the existance of the component
        self.session.tracer.trace(TraceNewComponent(
            component_id = self.component_id,
            name = self.name,
            clsname = self.__class__.__name__,
            state = ComponentTrace.INIT_STATE
        ))

        # Push some component info into connections to assist with tracing
        for name, port in self.ports:
            port._component_name = self.name
            port._port_name = name

            # Record port name in trace
            self.session.tracer.trace(TraceComponentPortExists(
                component_id = self.component_id,
                port_id = port.port_id,
                name = name,
                port_type = port.etl_port_type))

            # Trace connections now that session has been attached to ports
            if port.is_etl_output_port:
                port.trace_connections()





    def _child_etl_objects(self):
        for name, port in self.ports:
            yield port


    def _execute(self):
        '''Top level method in running thread / process'''
        self._startup()

        self.session.tracer.trace(TraceComponentStateChange(
            component_id = self.component_id,
            state = ComponentTrace.RUNNING_STATE))

        self.run()

        self.session.tracer.trace(TraceComponentStateChange(
            component_id = self.component_id,
            state = ComponentTrace.FINISHED_STATE))

        self._finish()


    def _startup(self):
        self.logger.info("Component started")

    def _finish(self):
        self.logger.info("Component finished")

        # TODO: Close inputs too just in case

        # Close our outputs
        for port in self.outputs:
            port.close()



    @property
    def is_etl_component(self):
        return True
