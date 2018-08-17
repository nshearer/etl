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



class TraceEvent(ABC):
    '''Object to trace activity while ETL is running'''

    def __init__(self, **kwargs):
        '''
        If tracing activity in ETL, then provide data keys in _list_required_keys()

        If restoring from trace file stream, specify restore_data keyword arg

        :param kwargs:
        '''
        self.ts = None
        self.__data_keys = None
        #
        # # Restoring from trace file?
        # if 'restore_data' in kwargs:
        #     data = kwargs['restore_data']
        #     if data['event_class'] != self.__class__.__name__:
        #         raise Exception("%s class given data for %s" % (
        #             self.__class__.__name__, data['event_class']))
        #     self.__dict__.update(data[''])
        #     self.__data_keys = list(data.keys())
        #
        #     # Note: if restoring from stream, then assume None for missing keys
        #     for missing_key in set(self._list_required_keys()) - set(self.__dict__.keys()):
        #         self.__dict__[missing_key] = None
        #         self.__data_keys.append(missing_key)
        #
        # # Else, take trace data from kwargs (used when tracing activity in ETL)
        # else:

        if True:
            if set(kwargs.keys()) != set(self._list_required_keys()):
                raise MissingTraceActionKwarg(self,
                    set(self._list_required_keys()), set(kwargs.keys()))
            self.__dict__.update(kwargs)
            self.__data_keys = list(kwargs.keys())

        # Record trace timestamp
        if self.ts is None:
            self.ts = datetime.now()


    @property
    @abstractmethod
    def _list_required_keys(self):
        '''
        List kwargs required to instantiate this trace object

        :return: set of keys
        '''

    @property
    def data_keys(self):
        return iter(self.__data_keys)


    # TODO: implement apply_to_trace_data()

    # @abstractmethod
    # def apply_to_trace_data(self, trace_data):
    #     '''
    #     Apply this trace event to the TraceData class
    #
    #     Assume write lock is already obtained
    #
    #     :param trace_data: TraceData collecting all trace data from trace file
    #     '''
    def apply_to_trace_data(self, trace_data):
        pass