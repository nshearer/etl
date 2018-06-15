'''
Created on Dec 27, 2012

@author: nshearer
'''
from UserDict import DictMixin
from threading import Lock

from netl.schema.EtlSchema import RecordSchemaError

from .exceptions import EtlRecordFrozen

NEXT_ETL_RECORD_SERIAL = 0
NEXT_ETL_RECORD_LOCK = Lock()

class EtlRecordSerial(object):
    '''Unique identification for EtlRecords'''
    
    def __init__(self):
        global NEXT_ETL_RECORD_SERIAL, NEXT_ETL_RECORD_LOCK
        with NEXT_ETL_RECORD_LOCK:
            self.__value = NEXT_ETL_RECORD_SERIAL
            NEXT_ETL_RECORD_SERIAL += 1

    def __str__(self):
        return str(self.__value)
    
    def __eq__(self, serial):
        return self.__value == serial.__value
    
    def __hash__(self):
        return hash(self.__value) # May change to string in the future
        


class EtlRecord(DictMixin):
    '''Container for values for a single record
    
    ETL Records are meant to not be mutable once they have been added to an
    output set.
    '''
    
    def __init__(self, schema, values=None):
        '''Init
        
        @param schema: The Schema this record is being created to match
        @param values: Initial values
        '''
        self.__values = dict()
        self.__schema = schema
        self.__serial = EtlRecordSerial()
        self.__frozen = False
        self.__src_processor = None
        self.__src_port = None
        self.__from_records = list()
        self.__size_cache = None
        
        if values is not None:
            for k, v in list(values.items()):
                self[k] = v
        
        
    def clone(self):
        return EtlRecord(self.__schema, self.__values)
    
    
    @property
    def serial(self):
        '''Unique identification of this record'''
        return self.__serial
    
        
    def field_names(self):
        if self.__schema is not None:
            return self.__schema.etl_list_field_names()
        return list(self.values.keys())
    

    # -- Source record linking.  TODO: Move? ----------------------------------
     
    def note_src_record(self, rec):
        '''Note another record that was processed to help create this record'''
        self.assert_not_frozen()
        if len(self.__from_records) < 100000:
            self.__from_records.append(rec.serial)
             
             
    def get_src_record_serials(self):
        '''Serial codes of records that helped generate this record'''
        return self.__from_records[:]
     
    # -- Handling fields ------------------------------------------------------
    
    def set_field_value(self, name, value):
        self.assert_not_frozen()
        
        # Validate field exists in schema
        schema_element = self.schema.etl_get_field(name)
        if schema_element is None:
            raise RecordSchemaError(
                record = self,
                field = name,
                error = "Field not in schema")
        
        self.__values[name] = self.schema[name].validate_and_set_value(value)
        
        
    def get_field_value(self, name):
        
        # Validate field exists in schema
        schema_element = self.schema.etl_get_field(name)
        if schema_element is None:
            raise RecordSchemaError(
                record = self,
                field = name,
                error = "Field not in schema")
            
        # See if a value is even stored
        if name not in self.__values or self.__values[name] is None:
            return self.schema[name].get_none_value(self.__frozen)
        
        # Return value
        stored_value = self.__values[name]
        return self.schema[name].access_value(stored_value, self.__frozen)
        

    # -- Source processor -----------------------------------------------------
    
    def set_source(self, prc_name, output_port_name):
        self.assert_not_frozen()
        self.__src_processor = prc_name
        self.__src_port = output_port_name
        
        
    @property
    def source_processor_name(self):
        return  self.__src_processor
    
    
    @property
    def source_processor_output_name(self):
        return self.__src_port
    

    # -- Debug ---------------------------------------------------------------
    
    def create_msg(self, msg):
        '''Generate a message about this record'''
        return "%s: %s: Record[[%s]]" % (msg, self.__serial, str(self.__values))
        
        
    @property
    def values(self):
        rtn = dict()
        for field_name in self.schema.etl_list_field_names():
            rtn[field_name] = self.get_field_value(field_name)
        return rtn
    
    
    def __getitem__(self, name):
        return self.get_field_value(name)
    
    
    def __setitem__(self, name, value):
        self.set_field_value(name, value)
        
        
    def set(self, name, value):
        self[name] = value
        
        
    def keys(self):
        return self.field_names()
    
    
    def freeze(self):
        self.__frozen = True
        for field_name in self.schema.etl_list_field_names():
            if field_name in self.__values:
                if self.__values[field_name] is not None:
                    value = self.__values[field_name]
                    value = self.schema[field_name].freeze_value(value)
                    self.__values[field_name] = value
        
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
            return self.__size_cache
        else:
            return self._calc_size()


    def _calc_size(self):
        '''Estimate records size'''
        size = 0
        for k, v in list(self.__values.items()):
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
        
        
    def __eq__(self, record):
        if record is None:
            return False

        try:        
            my_fields = sorted(self.field_names())
            rec_fields = sorted(record.field_names())
            if my_fields != rec_fields:
                return False
            
            for field_name in my_fields:
                if self[field_name] != record[field_name]:
                    return False
                
            return True
        
        except AttributeError:
            return False
        
        return False