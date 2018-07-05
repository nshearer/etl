from abc import ABC, abstractmethod

from .resources import EtlResourceCollection
from .EtlTracer import EtlTracer

class EtlSession:
    '''Holds information for a single execution of an ETL that is needed by all components'''

    def __init__(self):
        self.run_dir = None
        self.tracer = EtlTracer()
        self.resources = EtlResourceCollection()


class EtlObject(ABC):
    '''Base object for other classes that will work with ETL session'''

    @property
    def session(self):
        '''ETL Session with ETL execution info'''
        try:
            return self.__session
        except AttributeError:
            raise Exception("No session supplied.  was setup_etl() called?")


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
