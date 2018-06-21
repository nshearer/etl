
from threading import Lock

class EltOutputConnection:
    def __init__(self, input):
        self.input = input
        self.tok = None


class EtlRecordEnvelope:
    '''Wrapper around records specifying where they came from, and where they're going'''


class EtlPort:

    PORT_ID_LOCK = Lock()
    NEXT_PORT_ID = 0

    @staticmethod
    def new_unique_id():
        '''Get a unique port ID (used for outputs and inputs)'''
        with EtlPort.PORT_ID_LOCK:
            uid = EtlPort.NEXT_PORT_ID
            EtlPort.NEXT_PORT_ID += 1
        return uid



class EtlOutput(EtlPort):
    '''
    Defines an output channel for a component to send processed records out on
    '''

    def __init__(self):
        self.__mute_lock = Lock()
        self.__connections = list()
        self.__id = self.new_unique_id()

        # See EtlComponent.setup()
        self._src_component_name = None
        self._output_name = None


    @property
    def is_etl_output_port(self):
        return True
    @property
    def port_type(self):
        return 'o'


    def connect(self, input):
        '''Connect to the input of a component'''
        if input.port_type == 'o':
            raise Exception("Can't connect an output to an output")
        with self.__mute_lock:
            conn = EltOutputConnection(input)
            conn.tok = input._new_connection_token()
            self.__connections.append(conn)


    def output(self, record):
        record.freeze()
        record.set_source(self._src_component_name, self._output_name)
        for conn in self.__connections:
            conn.input._queue.put(record)