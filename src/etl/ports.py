from abc import ABCMeta, abstractmethod
from threading import Lock

STATUS_OPEN = 1         # Could send more records on this connection
STATUS_CLOSSED = 0      # Will not send more records

from exceptions import InvalidDataPortName

# -- Connections --------------------------------------------------------------
 
class EtlOutputConnection(object):
    '''Holds details about a connection to another processor's input port'''

    def __init__(self):
        self.status = STATUS_OPEN
        self.target_prc = None
        self.target_port_name = None

    @property
    def is_open(self):
        return self.status == STATUS_OPEN
    

class EtlInputConnection(object):
    '''Holds details about a manager connected to an input'''

    def __init__(self):
        self.status = STATUS_OPEN
        self.source_prc_name = None
        self.source_port_name = None

    @property
    def is_open(self):
        return self.status == STATUS_OPEN
    

# -- Base Classes -------------------------------------------------------------
 
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
    
    
    def __str__(self):
        return "%s('%s')" % (self.__class__.__name__, self.__name) 


class PortCollection(object):
    '''Base class for InputPorts and OutputPorts'''
    __metaclass__ = ABCMeta    

    def __init__(self):
        self._ports = dict()

    @abstractmethod
    def create_port(self, name):
        '''Define a new port'''
        
        
    def __getitem__(self, name):
        if not self._ports.has_key(name):
            raise InvalidDataPortName(name, self._ports.keys())
        return self._ports[name]


    def keys(self):
        return self._ports.keys()


    def values(self):
        return self._ports.values()



# -- Inputs -------------------------------------------------------------------
 
class InputPort(PortBase):
    '''Define a port that a component can recieve records on'''

    def __init__(self, name):
        super(InputPort, self).__init__(name)
        self.__conns = list()

        # Used to lock input so that sending processors get blocked:
        self.input_lock = Lock()
        
    def connected_by(self, processor_name, port):
        '''Record that this port has been connected to
        
        @param processor: Name of surce processor that connected to this port
        @param port: Name of output port on source processor dispatching
            records to this input port
        '''
        conn = EtlInputConnection()
        conn.status = STATUS_OPEN
        conn.source_prc_name = processor_name
        conn.source_port_name = port
        
        self.__conns.append(conn)
        
        
    @property
    def is_connected(self):
        '''Are any processors still sending input to this port'''
        for conn in self.__conns:
            if conn.is_open:
                return True
        return False


class InputPortCollection(PortCollection):
    '''Collection of all input ports for a processor'''

    def __init__(self):
        super(InputPortCollection, self).__init__()
        
    def create_port(self, name):
        '''Define a new input port'''
        self._ports[name] = InputPort(name)


# -- Outputs ------------------------------------------------------------------

class OutputPort(PortBase):
    '''Define a port that a component can dispatch records on'''

    def __init__(self, name):
        super(OutputPort, self).__init__(name)
        self.__conns = list()
        
        
    def connect_to(self, processor, port):
        '''Connect this output to the input port on the given processor
        
        @param processor: Processor object to receive dispatched records
        @param port: Name of input port on processor to dispatch records to
        '''
        conn = EtlOutputConnection()
        conn.status = STATUS_OPEN
        conn.target_prc = processor
        conn.target_port_name = port
        
        self.__conns.append(conn)
        
    
    def list_connections(self):
        for conn in self.__conns:
            yield conn.target_prc.name, conn.target_port_name


class OutputPortCollection(PortCollection):
    '''Collection of all input ports for a processor'''

    def __init__(self):
        super(OutputPortCollection, self).__init__()
        
    def create_port(self, name):
        '''Define a new output port'''
        self._ports[name] = OutputPort(name)



