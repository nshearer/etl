
from threading import Lock
from queue import Queue

class EtlInput:
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
    Once all of teh outputs that have connected to this input have signaled that they are
    "closing" the connected (i.e.: will not send any more records), then this input is
    considered closed and will not accept any additional records.

    '''

    DEFAULT_MAXSIZE = 1000

    UNCONNECTED = 'U'   # Nothing has been connected yet
    CONNECTED = 'C'     # At least 1 output has been connected to this input
    CLOSED = 'X'        # All components

    def __init__(self, maxsize=DEFAULT_MAXSIZE):
        self.__mute_lock = Lock()
        self._queue = Queue(maxsize)
        self.__state = self.UNCONNECTED
        self.__connection_tokens = set()
        self.__next_token = 0


    @property
    def port_type(self):
        return 'i'


    def connect(self, output):
        '''Connect an output to this input'''
        if output.port_type == 'i':
            raise Exception("Can't connect an input to an input")
        output.connect(self) # Do all connections output -> input


    def _new_connection_token(self):
        '''
        Request a connection token to "connect" to this input.  Called by EtlOutput
        '''
        with self.__mute_lock:
            tok = self.__next_token
            self.__next_token += 1
            self.__connection_tokens.add(tok)
            return tok


    def all(self):
        '''Return all records'''
        while True:
            yield self._queue.get()


    def get(self):
        '''Return a single record'''
        return self._queue.get()