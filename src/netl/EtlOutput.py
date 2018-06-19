
from threading import Lock

class EltOutputConnection:
    def __init__(self, input):
        self.input = input
        self.tok = None


class EtlOutput:
    '''
    Defines an output channel for a component to send processed records out on
    '''


    def __init__(self):
        self.__mute_lock = Lock()
        self.__connections = list()

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