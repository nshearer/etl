
from threading import Lock
from queue import Queue

from .EtlOutput import EtlPort
from .exceptions import NoMoreData
from .tracedb import TracePortClosed

class EtlInput(EtlPort):
    '''
    Defines an input channel for a component to receive records on.

    Each connection gets it's own process-safe queue to receive records on.  Outputs from
    other components are connected to this input which allows components to pass data
    (as EtlRecords) into another component.

    Connections:
    When an output is connected to an input, it is passed a connection token.  This is
    primarily to detect the error condition when an output tries to send more data on
    a connection it has clossed.

    Closed Inputs:
    Once all of the outputs that have connected to this input have signaled that they are
    "closing" the connected (i.e.: will not send any more records), then this input is
    considered closed and will not accept any additional records.

    '''

    DEFAULT_MAXSIZE = 1000

    UNCONNECTED = 'U'   # Nothing has been connected yet
    CONNECTED = 'C'     # At least 1 output has been connected to this input
    CLOSED = 'X'        # All components

    def __init__(self, maxsize=DEFAULT_MAXSIZE, class_port=True):
        super(EtlInput, self).__init__(class_port=class_port)

        self.__max_queue_size = maxsize

        if not self.is_class_port:
            self.__mute_lock = Lock()
            self._queue = Queue(maxsize)
            self.__state = self.UNCONNECTED
            self.__connection_tokens = set()
            self.__next_token = 0

        # See EtlComponent.setup()
        self._component_name = None
        self._port_name = None


    def create_instance_port(self):
        return EtlInput(maxsize=self.__max_queue_size, class_port=False)


    @property
    def etl_port_type(self):
        return 'i'


    def connect(self, output):
        '''Connect an output to this input'''

        self.assert_is_instance_port()

        try:
            if output.is_etl_input_port:
                raise Exception("Can't connect an input to an input")
        except AttributeError:
            pass

        try:
            if not output.is_etl_output_port:
                raise Exception("Can't connect an input to non-output port")
        except AttributeError:
            raise Exception("Can't connect an input to a %s" % (output.__class__.__name__))

        output.connect(self) # Do all connections output -> input


    def _new_connection_token(self):
        '''
        Request a connection token to "connect" to this input.  Called by EtlOutput
        '''
        self.assert_is_instance_port()
        with self.__mute_lock:
            tok = self.__next_token
            self.__next_token += 1
            self.__connection_tokens.add(tok)
            return tok


    def all(self, envelope=False):
        '''Return all records'''
        try:
            while True:
                yield self.get(envelope)
        except NoMoreData:
            return


    def get(self, envelope=False):
        '''Return a single record'''

        self.assert_is_instance_port()

        unwrap = not envelope

        # Make sure we have a connection
        if len(self.__connection_tokens) == 0:
            self.session.tracer.trace(TracePortClosed(port_id=self.port_id))
            raise NoMoreData("Input has no open connections")

        envelope = self._queue.get()
        envelope.note_receiver(self._component_name, self._port_name)

        if envelope.msg_type == 'record':
            self.session.tracer.trace_record_rcvd(envelope)
            if unwrap:
                return envelope.record
            else:
                return envelope

        elif envelope.msg_type == 'close':
            connection_token = envelope.record
            self.__connection_tokens.remove(connection_token)
            return self.get(not unwrap) # Will raise NoMoreData() if no more connections


