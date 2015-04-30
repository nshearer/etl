from abc import ABCMeta, abstractmethod
from threading import Lock

STATUS_OPEN = 1         # Could send more records on this connection
STATUS_CLOSSED = 0      # Will not send more records


class EtlOutputConnection(object):
    '''Holds details about a connection to another processor's input port'''

    def __init__(self):
        self.status = STATUS_CLOSSED
        self.target_prc_name = None
        self.target_prc = None
        self.target_prc_port_name = None
        
        self.prc_manager = None
        self.schema = None
        self.event_queue = None
        self.record_queue = None


class EtlInputConnection(object):
    '''Holds details about a manager connected to an input'''

    def __init__(self):
        self.conn_id = None
        self.status = None
        self.prc_name = None
        self.port_name = None
        self.schema = None


class PortBase(object):
    '''Base class for InputPortCollection and OutputPortCollection'''
    __metaclass__ = ABCMeta

    def __init__(self, name):
        '''Init 

        @param name: Name of the port for forming connections
        '''
        self.__name = name


    @property
    def name(self):
        return self.__name


class PortCollection(object):
    '''Base class for InputPorts and OutputPorts'''
    __metaclass__ = ABCMeta    

    def __init__(self):
        self._ports = dict()

    @abstractmethod
    def create_port(self, name):
        '''Define a new port'''



class InputPort(PortBase):
    '''Define a port that a component can recieve records on'''

    def __init__(self, name):
        super(InputPort, self).__init__(name)

        # Used to lock input so that sending processors get blocked:
        self.input_lock = Lock()


class InputPortCollection(PortCollection):
    '''Collection of all input ports for a processor'''

    def __init__(self):
        super(InputPortCollection, self).__init__()
        
    def create_port(self, name):
        '''Define a new input port'''
        self._ports[name] = InputPort(name)


class OutputPort(PortBase):
    '''Define a port that a component can dispatch records on'''

    def __init__(self, name):
        super(OutputPort, self).__init__(name)


class OutputPortCollection(PortCollection):
    '''Collection of all input ports for a processor'''

    def __init__(self):
        super(OutputPortCollection, self).__init__()
        
    def create_port(self, name):
        '''Define a new output port'''
        self._ports[name] = OutputPort(name)



