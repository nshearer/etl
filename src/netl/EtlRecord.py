'''
Created on Dec 27, 2012

@author: nshearer
'''
from collections import OrderedDict
from pprint import pformat

from .exceptions import EtlRecordFrozen, InvalidEtlRecordKey
from .exceptions import NoFreezeFunction, ValueFreezeFailed
from .serial import EtlSerial
from datetime import datetime, date


def repr_attr_value(value):
    '''
    Create a representation of the value of an attribute for the TraceDB

    Since the TraceDB is only used for monitoring and debugging, this needs
    to turn each value into a string a person can understand.

    :param value:
    :return: str
    '''

    if value is None:
        return 'none'

    if value.__class__ is datetime:
        return value.strftime("%Y-%m-%d %H:%M:%S")
    elif value.__class__ is date:
        return value.strftime("%Y-%m-%d")

    return pformat(value).strip("'")


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
        self.__values = OrderedDict()
        self.__frozen = False

        self.__origin_comp_id = None
        self.__origin_comp_name = None
        self.__origin_port = None

        self.__from_records = list()
        self.__size_cache = None

        if serial == 'NEXT':
            self.__serial = EtlSerial()

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


    @property
    def serial(self):
        return self.__serial


    def freeze(self, freezer):
        '''
        Freeze and record to prevent accidental updates

        :param freezer: EtlRecordFreezer for freezing values
        '''

        if self.frozen:
            return

        if self.__serial is None:
            self.__serial = EtlSerial()

        for key, value in self.__values.items():
            self.__values[key] = freezer.freeze(value)

        self.__frozen = True


    def copy(self):
        '''
        Make a copy of the record that can be updated

        :return:
        '''
        if self.frozen:
            return EtlRecord(
                record_type = self.record_type,
                serial = EtlSerial(),
                **self.__values)
        else:
            raise Exception("Freeze record before copy()")
        # TODO: automatically associate derived record?


    # # -- Source record linking.  TODO: Move? ----------------------------------
    #
    # def note_src_record(self, rec):
    #     '''Note another record that was processed to help create this record'''
    #     self.assert_not_frozen()
    #     if len(self.__from_records) < 100000:
    #         self.__from_records.append(rec.serial)
    #
    #
    # def get_src_record_serials(self):
    #     '''Serial codes of records that helped generate this record'''
    #     return self.__from_records[:]
    #

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


    @property
    def attributes(self):
        '''
        All attributes for this record

        :return: (key, value) pairs
        '''
        return self.__values.items()


    # -- Source processor -----------------------------------------------------

    def set_source(self, comp_id, comp_name, output_port_name):
        self.assert_not_frozen()
        self.__origin_comp_id = comp_id
        self.__origin_comp_name = comp_name
        self.__origin_port = output_port_name


    @property
    def origin_component_id(self):
        return  self.__origin_comp_id


    @property
    def origin_component_name(self):
        return  self.__src_comp_name


    @property
    def origin_output_name(self):
        return self.__src_port


    # -- Debug ---------------------------------------------------------------

    def create_msg(self, msg):
        '''Generate a message about this record'''
        return "%s: %s: Record[[%s]]" % (msg, self.__serial, str(self.__values))


    def format(self, width=80, header=True, border=True):
        '''
        Create a nice, multiline, printable representation of the record

        :param width: Max width to use for formatting record
        :param header: Whether to include record header (else, just values)
        :param border: Whether to wrap record in a boarder
        :return: str
        '''
        rtn = list()

        attrs = [(k, repr_attr_value(v)) for (k, v) in self.__values.items()]

        max_key_len = max([len(t[0]) for t in attrs])

        max_value_len = min((width - max_key_len, max([len(t[1]) for t in attrs])))
        if border:
            max_value_len -= 7

        line_len = max_key_len + max_value_len
        if border:
            line_len += 7 # '| ' + ' | ' + ' |'
        else:
            line_len += 2 # ': ' between

        if border:
            border = "+-" + '-'*(max_key_len) + '-+-' + '-'*(max_value_len) + '-+'

        if header:
            if border:
                rtn.append(border)

            header = "%s [%s]" % (self.record_type, self.serial)
            if len(header) < line_len - 4:
                header += " "*(line_len - len(header) - 4)
            if border:
                rtn.append('| ' + header + ' |')
        else:
            header = None

        if border:
            rtn.append(border)

        for key, value in attrs:
            if not border:
                key += ':'
            key = key + " "*(max_key_len-len(key))

            if len(value) > max_value_len:
                value = value[:max_value_len-2] + '..'
            else:
                value = value + " "*(max_value_len-len(value))

            if border:
                rtn.append("| %s | %s |" % (key, value))
            else:
                rtn.append("%s %s" % (key, value))

        if border:
            rtn.append(border)

        return "\n".join(rtn)


    # -- Attributes -----------------------------------------------------------


    def set(self, name, value):
        self[name] = value


    def keys(self):
        return self.field_names()


    @property
    def frozen(self):
        return self.__frozen


    def assert_not_frozen(self):
        if self.__frozen:
            raise EtlRecordFrozen()


    def __eq__(self, record):
        raise NotImplementedError("TODO")