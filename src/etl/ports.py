from abc import ABCMeta, abstractmethod
from threading import Lock


class EtlOutputConnection(object):
    '''Holds details about a connected output manager'''
    def __init__(self):
        self.conn_id = None
        self.status = None
        self.prc_name = None
        self.port_name = None
        
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
        self.__ports = dict()

    @abstractmethod
    def create_port(self, name):
        '''Define a new port'''



class InputPort(PortBase):
    '''Define a port that a component can recieve records on'''


class InputPortCollection(PortCollection):
    '''Collection of all input ports for a processor'''

    def __init__(self, name):
        super(InputPortCollection, self).__init__(name)
        
        # Used to lock input so that sending processors get blocked:
        self.input_lock = Lock()    



class OutputPort(PortBase):
    '''Define a port that a component can dispatch records on'''


class OutputPortCollection(PortCollection):
    '''Collection of all input ports for a processor'''

