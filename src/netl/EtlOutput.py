from abc import abstractmethod

from threading import Lock
from .EtlSession import EtlObject
from .tracefile import TracePortClosed, TraceConnection, TraceConnectionClosed
from .tracefile import TraceRecord
from .tracefile import TraceRecordDispatch
from .exceptions import SessionNotCreatedYet

class OutputClossed(Exception): pass

class EltOutputConnection:
    def __init__(self, input):
        self.to_port = input
        self.tok = None
        self.traced = False
# TODO: Add connection ID in case someone wants to connect two ports twice?
#       Would need to update traces too

class EtlRecordEnvelope:
    '''Wrapper around records specifying where they came from, and where they're going'''
    def __init__(self, msg_type, from_comp_name, from_comp_id, from_port_name, from_port_id, record):
        self.__msg_type = msg_type
        self.__from_comp_name = from_comp_name
        self.__from_comp_id = from_comp_id
        self.__from_port_id = from_port_id
        self.__from_port_name = from_port_name

        self.__record = record

        self.__to_comp_name = None
        self.__to_comp_id = None
        self.__to_port_name = None
        self.__to_port_id = None

    def note_receiver(self, comp_name, comp_id, port_name, port_id):
        self.__to_comp_name = comp_name
        self.__to_comp_id = comp_id
        self.__to_port_name = port_name
        self.__to_port_id = port_id

    def __str__(self):
        return "{from_comp}.{output_port} -> {to_comp}.{input_port}".format(
            from_comp = self.from_comp_name,
            output_port = self.from_port_name,
            to_comp = self.to_comp_name or '?',
            input_port = self.to_port_name or '?')

    @property
    def msg_type(self):
        return self.__msg_type
    @property
    def from_comp_name(self):
        return self.__from_comp_name
    @property
    def from_comp_id(self):
        return self.__from_comp_id
    @property
    def from_port_id(self):
        return self.__from_port_id
    @property
    def from_port_name(self):
        return self.__from_port_name
    @property
    def record(self):
        return self.__record
    @property
    def to_comp_name(self):
        return self.__to_comp_name
    @property
    def to_comp_id(self):
        return self.__to_comp_id
    @property
    def to_port_name(self):
        return self.__to_port_name
    @property
    def to_port_id    (self):
        return self.__to_port_id


class EtlPort(EtlObject):

    PORT_ID_LOCK = Lock()
    NEXT_PORT_ID = 0

    def __init__(self, class_port=True):
        '''
        :param class_port:
            Set to true if this is a port being defined on a class
            (Requires replacment of port object with create_instance() before use)
        '''

        # See EtlComponent.setup()
        self._component_id = None
        self._component_name = None
        self._port_name = None
        self.__class_port = class_port

        # Calculate port ID
        if not self.__class_port:
            with EtlPort.PORT_ID_LOCK:
                self.__port_id = EtlPort.NEXT_PORT_ID
                EtlPort.NEXT_PORT_ID += 1


    def __str__(self):
        return "%s.%s" % (
            self._component_name or "UnknownComponent",
            self._port_name or "unknown_port",
        )


    @property
    def is_class_port(self):
        '''Is this a port on a component class (can't be connected)'''
        return self.__class_port


    def assert_is_instance_port(self):
        '''Check that this is a usable port on a component instance (not on the class)'''
        if self.__class_port:
            raise Exception("".join((
                "Can't call this method on a class %s port.  " % (self.etl_port_type),
                "Either EtlComponent.__init__() wasn't called, or ",
                "you're trying to call on a component class (not an instance)")))


    @abstractmethod
    def create_instance_port(self):
        '''
        Called in EtlComponent to create an instance of this port that can be used

        This was done because otherwise two copmnents that use the same component class
        ended up with the same port instances.
        '''


    @property
    def port_id(self):
        return self.__port_id


    @property
    def component_name(self):
        return self._component_name

    @property
    def name(self):
        return self._port_name


    def _child_etl_objects(self):
        return None

    @property
    def is_etl_port(self):
        return True

    @property
    @abstractmethod
    def etl_port_type(self):
        '''Input or output'''

    @property
    def is_etl_input_port(self):
        return self.etl_port_type == 'i'
    @property
    def is_etl_output_port(self):
        return self.etl_port_type == 'o'


class EtlOutput(EtlPort):
    '''
    Defines an output channel for a component to send processed records out on
    '''

    def __init__(self, class_port=True):
        super(EtlOutput, self).__init__(class_port=class_port)

        if not self.is_class_port:
            self.__mute_lock = Lock()
            self.__connections = list()

        self.__closed = False


    def create_instance_port(self):
        return EtlOutput(class_port=False)


    @property
    def etl_port_type(self):
        return 'o'


    def connect(self, input):
        '''Connect to the input of a component'''
        self.assert_is_instance_port()

        try:
            if not input.is_etl_input_port:
                raise Exception("Can't connect an output to an output")
        except AttributeError:
            raise Exception("Can't connect an output to %s" % (input.__class__.__name__))

        with self.__mute_lock:
            conn = EltOutputConnection(input)
            conn.tok = input._new_connection_token()
            self.__connections.append(conn)


    def trace_connections(self):
        try:
            for conn in self.__connections:
                if not conn.traced:
                    self.session.tracer.trace(TraceConnection(
                        from_port_id = self.port_id,
                        to_port_id = conn.to_port.port_id))
                    conn.traced = True
        except SessionNotCreatedYet:
            # Session isn't created yet when most connections are made (ETL not started)
            # Called just in case a new connection is made after ETL is started
            pass


    def output(self, record):
        '''Send a record out on this output to any connected inputs'''

        self.assert_is_instance_port()

        # Check connection state

        if self.__closed:
            raise OutputClossed("Output %s.%s has been closed" % (
                self._component_name, self._port_name))

        try:
            if not record.is_etl_record:
                raise AttributeError()
        except AttributeError:
            raise Exception("Expected an EtlRecord, got %s" % (str(type(record))))

        # Freeze the record
        if not record.frozen:
            record.set_source(self._component_id, self._component_name, self._port_name)
            record.attach_attr_handler(self.session.attribute_handler)
            record.freeze()
            self.session.tracer.trace(TraceRecord(
                type=   record.record_type,
                serial= int(str(record.serial)),
                attrs=  list(record.repr_attrs())))

        # Send the record
        for conn in self.__connections:
            # Note: Not specifying receiver names here because receiving component
            #       may not have been started yet.  See EtlInput.get()

            # Wrap record in envelope for target component and send
            conn.to_port._queue.put(EtlRecordEnvelope(
                msg_type        = 'record',
                from_comp_name  = self._component_name,
                from_comp_id    = self._component_id,
                from_port_name  = self._port_name,
                from_port_id    = self.port_id,
                record          = record,
            ))

            # Trace message
            self.session.tracer.trace(TraceRecordDispatch(
                from_comp_name  = self._component_name,
                from_comp_id    = self._component_id,
                from_port_name  = self._port_name,
                from_port_id    = self.port_id,
                record_id       = record.serial,
                to_comp_id      = conn.to_port._component_id,
                to_comp_name    = conn.to_port._component_name,
                to_port_name    = conn.to_port._port_name,
                to_port_id      = conn.to_port.port_id))




    def close(self):
        '''Close down output and signal connected inputs that no more records will be sent'''

        self.assert_is_instance_port()

        self.__closed = True
        for conn in self.__connections:
            conn.to_port._queue.put(EtlRecordEnvelope(
                msg_type        = 'close',
                from_comp_name  = self.component_name,
                from_comp_id    = self._component_id,
                from_port_name  = self.name,
                from_port_id    = self.port_id,
                record          = conn.tok,
              ))
            self.session.tracer.trace(TraceConnectionClosed(
                from_port_id=self.port_id,
                to_port_id=conn.to_port.port_id))
        self.session.tracer.trace(TracePortClosed(
            component_id = self._component_id,
            port_id = self.port_id))
