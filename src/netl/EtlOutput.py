from abc import abstractmethod

from threading import Lock
from .EtlSession import EtlObject
from .tracedb import TracePortClosed, TraceConnection, TraceConnectionClosed
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
    def __init__(self, msg_type, from_comp, from_port, record):
        self.__msg_type = msg_type
        self.__from_comp = from_comp
        self.__from_port = from_port
        self.__record = record
        self.__to_comp = None
        self.__to_port = None
    def note_receiver(self, comp_name, port_name):
        self.__to_comp = comp_name
        self.__to_port = port_name
    def __str__(self):
        return "%s.%s -> %s.%s" % (self.from_comp, self.from_port, self.to_comp or '?', self.to_port or '?')
    @property
    def msg_type(self):
        return self.__msg_type
    @property
    def from_comp(self):
        return self.__from_comp
    @property
    def from_port(self):
        return self.__from_port
    @property
    def to_comp(self):
        return self.__to_comp
    @property
    def to_port(self):
        return self.__to_port
    @property
    def record(self):
        return self.__record


class EtlPort(EtlObject):

    PORT_ID_LOCK = Lock()
    NEXT_PORT_ID = 0

    def __init__(self):
        # See EtlComponent.setup()
        self._component_name = None
        self._port_name = None

        with EtlPort.PORT_ID_LOCK:
            self.__port_id = EtlPort.NEXT_PORT_ID
            EtlPort.NEXT_PORT_ID += 1


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

    def __init__(self):
        super(EtlOutput, self).__init__()
        self.__mute_lock = Lock()
        self.__connections = list()
        self.__closed = False

    @property
    def etl_port_type(self):
        return 'o'


    def connect(self, input):
        '''Connect to the input of a component'''
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

        if self.__closed:
            raise OutputClossed("Output %s.%s has been closed" % (
                self._component_name, self._port_name))

        try:
            if not record.is_etl_record:
                raise AttributeError()
        except AttributeError:
            raise Exception("Expected an EtlRecord, got %s" % (str(type(record))))

        record.freeze()

        for conn in self.__connections:
            # Note: Not specifying receiver names here because receiving component
            #       may not have been started yet.  See EtlInput.get()
            conn.to_port._queue.put(EtlRecordEnvelope(
                msg_type    = 'record',
                from_comp   = self._component_name,
                from_port   = self._port_name,
                record      = record,
            ))


    def close(self):
        '''Close down output and signal connected inputs that no more records will be sent'''

        self.__closed = True
        for conn in self.__connections:
              conn.to_port._queue.put(EtlRecordEnvelope(
                msg_type    = 'close',
                from_comp   = self.component_name,
                from_port   = self.name,
                record      = conn.tok,
              ))
              self.session.tracer.trace(TraceConnectionClosed(
                  from_port_id=self.port_id,
                  to_port_id=conn.to_port.port_id))
        self.session.tracer.trace(TracePortClosed(port_id=self.port_id))
