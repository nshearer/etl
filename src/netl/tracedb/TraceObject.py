from abc import ABC, abstractmethod
from datetime import datetime

class TraceData:
    '''An object stored in the trace database'''

    def __init__(self, trace_db, **attrs):
        self.db = trace_db
        self.__dict__.update(attrs)


class TraceAction(ABC):
    '''Object to trace activity while ETL is running'''

    def __init__(self):
        self.ts = datetime.now()

    # @abstractmethod
    # def __str__(self):
    #     '''Description of the traced activity'''

    @abstractmethod
    def record_trace_to_db(self, trace_db, commit):
        '''
        Record the trace to DB

        :param trace_db: TraceDB
        :param commit:
            Advice whether it's time to commit after this operation
        '''