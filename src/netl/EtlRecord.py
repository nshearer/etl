'''
Created on Dec 27, 2012

@author: nshearer
'''
import io
import pickle
from threading import Lock

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
        

# TODO: When I freeze a record, maybe produce a new object with the pickked
#       data in it to pass?

class EtlRecord:
    '''Container for values for a single record
    
    ETL Records are meant to not be mutable once they have been added to an
    output set.
    '''

    def __init__(self, **values):
        '''Init
        
        @param schema: The Schema this record is being created to match
        @param values: Initial values
        '''
        self.__values = dict()
        self.__serial = EtlRecordSerial()
        self.__frozen = False
        self.__src_processor = None
        self.__src_port = None
        self.__from_records = list()
        self.__size_cache = None

        self.__frozen_data = None

        if values is not None:
            for k, v in list(values.items()):
                self[k] = v


    def clone_for_edit(self):
        if self.__frozen_data is not None:
            copy = pickle.loads(self.__frozen_data)
            copy.__serial = EtlRecordSerial()
            return copy
        return EtlRecord(self.__values)
        # TODO: automatically associate derived record?


    @property
    def serial(self):
        '''Unique identification of this record'''
        return self.__serial


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

    def __setitem__(self, name, value):
        self.assert_not_frozen()
        self.__values[name] = value

    def __getitem__(self, name):
        return self.__values[name]


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


    def set(self, name, value):
        self[name] = value


    def keys(self):
        return self.field_names()


    def freeze(self):
        self.assert_not_frozen()
        self.__frozen = True
        self.__frozen_data = pickle.dumps(self)


    @property
    def is_frozen(self):
        return self.__frozen


    def assert_not_frozen(self):
        if self.__frozen:
            raise EtlRecordFrozen()


    def __eq__(self, record):
        raise NotImplementedError("TODO")