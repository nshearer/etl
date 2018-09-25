import os
from abc import ABC, abstractmethod
import logging
from tempfile import gettempdir

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
        self.attribute_handler = EtlAttributeHandler()
        self.attribute_handler.logger = self.get_logger('FREEZER')


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


    @property
    def temp_directory(self):
        '''Return path to directory to store temporary files in for this session'''
        if self.run_dir is not None:
            path = os.path.join(self.run_dir, 'tmp')
        else:
            path = gettempdir()

        if not os.path.exists(path):
            os.mkdir(path)

        return path



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


