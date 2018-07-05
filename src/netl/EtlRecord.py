'''
Created on Dec 27, 2012

@author: nshearer
'''
from threading import Lock

try:
    import ujson as json
except:
    import json

from .exceptions import EtlRecordFrozen, InvalidEtlRecordKey
from .serial import EtlSerial

class EtlRecord:
    '''Container for values for a single record
    
    ETL Records are meant to not be mutable once they have been dispached out
    of a component.
    '''

    def __init__(self, record_type, serial='NEXT', **values):
        '''Init
        
        @param record_type: Type of record (information only, no type checking)
        @param values: Initial values
        '''
        self.__serial = None
        self.__record_type = record_type
        self.__values = dict()
        self.__frozen = False
        self.__src_processor = None
        self.__src_port = None
        self.__from_records = list()
        self.__size_cache = None

        if serial == 'NEXT':
            self.__serial = EtlSerial()

        self.__frozen_json = None

        if values is not None:
            for k, v in list(values.items()):
                self[k] = v


    @property
    def is_etl_record(self):
        return True


    @property
    def record_type(self):
        return self.__record_type
    # TODO: Allow changes to record type?


    def freeze(self):
        self.assert_not_frozen()
        self.__frozen = True
        self.__frozen_json = json.dumps({
            'type':   self.__record_type,
            'serial': str(self.__serial),
            'values': self.__values})


    def copy(self):
        if self.__frozen_json is not None:
            info = json.loads(self.__frozen_json)
            copy = EtlRecord(info['type'], serial=EtlSerial(), **info['values'])
            return copy
        raise Exception("Freeze record before copy()")
        # TODO: automatically associate derived record?


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
        try:
            return self.__values[name]
        except KeyError:
            raise InvalidEtlRecordKey("%s record has no '%s' attribute" % (
                self.record_type, name))


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


    @property
    def is_frozen(self):
        return self.__frozen


    def assert_not_frozen(self):
        if self.__frozen:
            raise EtlRecordFrozen()


    def __eq__(self, record):
        raise NotImplementedError("TODO")