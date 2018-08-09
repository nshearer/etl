from abc import ABC, abstractmethod
import logging

from .resources import EtlResourceCollection
from .EtlTracer import EtlTracer
from .EtlAttributeHandler import EtlAttributeHandler

from .serial import EtlSerial
from .exceptions import SessionNotCreatedYet

class EtlSession:
    '''Holds information for a single execution of an ETL that is needed by all components'''

    def __init__(self):
        self.run_dir = None
        self.tracer = EtlTracer()
        self.tracer.logger = self.get_logger('TRACE')
        self.resources = EtlResourceCollection()
        self.attr_handler = EtlAttributeHandler()
        self.freezer.logger = self.get_logger('FREEZER')


    @property
    def session_id(self):
        '''Unique ID for this session (in case you get multiple ETLs running'''
        try:
            return self.__session_id
        except AttributeError:
            self.__session_id = int(str(EtlSerial()))
            return self.__session_id


    def get_logger(self, name=None):
        '''Get a logger for this session'''
        if name is None:
            return logging.getLogger('etl.%d' % (self.session_id))
        return logging.getLogger('etl.%d.%s' % (self.session_id, name))



class EtlObject(ABC):
    '''Base object for other classes that will work with ETL session'''

    @property
    def session(self):
        '''ETL Session with ETL execution info'''
        try:
            return self.__session
        except AttributeError:
            raise SessionNotCreatedYet("No session supplied.  was setup_etl() called?")


    def setup_etl(self, session):
        '''Prepare this object just prior to ETL execution'''
        try:
            if self.__session:
                raise Exception("Object already configured")
        except AttributeError:
            pass

        self.__session = session

        children = self._child_etl_objects()
        if children is not None:
            for child in children:
                child.setup_etl(session)

    @abstractmethod
    def _child_etl_objects(self):
        '''List sub objects that also need to have setup_etl() called (or None)'''


