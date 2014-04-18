'''
Created on Dec 27, 2012

@author: nshearer
'''
from UserDict import DictMixin
from threading import Lock

NEXT_ETL_RECORD_SERIAL = 0L
NEXT_ETL_RECORD_LOCK = Lock()

class EtlRecordSerial(object):
    '''Unique identification for EtlRecords'''
    
    def __init__(self):
        global NEXT_ETL_RECORD_SERIAL, NEXT_ETL_RECORD_LOCK
        with NEXT_ETL_RECORD_LOCK:
            self.__value = NEXT_ETL_RECORD_SERIAL
            NEXT_ETL_RECORD_SERIAL += 1L

    def __str__(self):
        return str(self.__value__)
    
    def __eq__(self, serial):
        return self.__value == serial.__value
    
    def __hash__(self):
        return hash(self.__value) # May change to string in the future
        


class EtlRecordFrozen(Exception):
    def __init__(self):
        msg = "Attempting to modify a frozen EtlRecord"
        super(EtlRecordFrozen, self).__init__(msg)        


class EtlRecord(DictMixin):
    '''Container for values for a single record
    
    ETL Records are meant to not be mutable once they have been added to an
    output set.
    '''
    
    def __init__(self, schema, values):
        '''Init
        
        @param schema: The Schema this record is being created to match
        @param values: Initial values
        '''
        self.__values = values.copy()
        self.__schema = schema
        self.__serial = EtlRecordSerial()
        self.__frozen = False
        self.__src_processor = None
        self.__src_port = None
        self.__from_records = list()
        self.__size_cache = None
        
        
    def clone(self):
        return EtlRecord(self.__schema, self.__values)
    
    
    @property
    def serial(self):
        '''Unique identification of this record'''
        return self.__serial
    
        
    def field_names(self):
        return self.values.keys()
    
    
    def note_src_record(self, rec):
        '''Note another record that was processed to help create this record'''
        self.assert_not_frozen()
        if len(self.__from_records) < 100000:
            self.__from_records.append(rec.serial)
    
    
    def set_source(self, prc_name, output_port_name):
        self.assert_not_frozen()
        self.__src_processor = prc_name
        self.__src_port = output_port_name
    
    
    def create_msg(self, msg):
        '''Generate a message about this record'''
        return "%s: %s: Record[[%s]]" % (msg, self.__serial, str(self.__values))
        
        
    @property
    def values(self):
        return self.__values.copy()
    
    
    def value(self, name):
        return self.__values[name]
    
    
    def __getitem__(self, name):
        return self.value(name)
    
    
    def __setitem__(self, name, value):
        self.assert_not_frozen()
        self.__values[name] = value
    
    
    def freeze(self):
        self.__frozen = True
        
    @property
    def is_frozen(self):
        return self.__frozen
    
    def assert_not_frozen(self):
        if self.__frozen:
            raise EtlRecordFrozen()
        
        
    @property
    def size(self):
        '''Estimate records size'''
        if self.__frozen:
            if self.__size_cache is None:
                self.__size_cache = self._calc_size()
        else:
            return self._calc_size()


    def _calc_size(self):
        '''Estimate records size'''
        size = 0
        for k, v in self.__values.items():
            size += len(k)
            if type(v) is str:
                size += len(v)
            else:
                size += 1
        return size


    @property
    def schema(self):
        return self.__schema
    def set_schema(self, new_schema):
        '''Replace schema
        
        Note: We allow the schema to be replaced to assist with storing to disk
        '''
        self.__schema = new_schema