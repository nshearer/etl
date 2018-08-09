from abc import ABC, abstractmethod
from datetime import datetime

try:
    import ujson as json
except ImportError:
    import json


class MissingTraceActionKwarg(Exception):
    def __init__(self, action, required_keys, present_keys):
        self.missing_keys = required_keys - present_keys
        msg = "Missing key(s) from %s: %s" % (
            action.__class__.__name__,
            ', '.join(self.missing_keys))
        super(MissingTraceActionKwarg, self).__init__(msg)


class TraceData(ABC):
    '''Object to trace activity while ETL is running'''

    def __init__(self, **kwargs):
        '''
        If tracing activity in ETL, then provide data keys in _list_required_keys()

        If restoring from trace file stream, specify restore_json keyword arg

        :param kwargs:
        '''
        self.ts = None
        self.__json = None
        self.__data_keys = None

        # Restoring from trace file?
        if 'restore_json' in kwargs:
            self.__json = kwargs['restore_json']
            data = json.loads(self.__json)
            self.__dict__.update(data)
            self.__data_keys = list(data.keys())

            # Note: if restoring from stream, then assume None for missing keys
            for missing_key in set(self._list_required_keys()) - set(self.__dict__.keys()):
                self.__dict__[missing_key] = None
                self.__data_keys.append(missing_key)

        # Else, take trace data from kwargs (used when tracing activity in ETL)
        else:
            if set(kwargs.keys()) != set(self._list_required_keys()):
                raise MissingTraceActionKwarg(self,
                    set(self._list_required_keys()), set(kwargs.keys()))
            self.__dict__.update(kwargs)
            self.__data_keys = list(kwargs.keys())

        # Record trace timestamp
        if self.ts is None:
            self.ts = datetime.now()
        self.__data_keys.append('ts')


    @property
    @abstractmethod
    def _list_required_keys(self):
        '''
        List kwargs required to instantiate this trace object

        :return: set of keys
        '''


    @property
    def data_json(self):
        '''JSON representation of this trace activity'''
        if self.__json is None:
            self.__json = json.dumps({k: getattr(self, k) for k in self.__data_keys})
        return self.__json


    @property
    def data_keys(self):
        return tuple(self.__data_keys)

