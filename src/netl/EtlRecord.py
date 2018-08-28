'''
Created on Dec 27, 2012

@author: nshearer
'''
from collections import OrderedDict
from pprint import pformat

from .exceptions import EtlRecordFrozen, InvalidEtlRecordKey
from .serial import EtlSerial
from datetime import datetime, date

from .EtlAttributeHandler import AttributeValue, FrozenAttributeValue

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
        self.__attr_handler = None

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
    @record_type.setter
    def record_type(self, value):
        self.assert_not_frozen()
        self.__record_type = value

    @property
    def rectype(self):
        return self.record_type

    @property
    def serial(self):
        return self.__serial


    # -- Handling fields ------------------------------------------------------

    def __setitem__(self, name, value):
        self.assert_not_frozen()

        if name not in self.__values:
            if not isinstance(value, AttributeValue):
                value = AttributeValue(value)
            self.__values[name] = value
        else:
            self.__values[name].set_value(value)


    def __getitem__(self, name):
        try:
            return self.__values[name].value
        except KeyError:
            raise InvalidEtlRecordKey("%s record has no '%s' attribute" % (
                self.record_type, name))


    @property
    def attributes(self):
        '''
        All attributes for this record

        :return: (key, value) pairs
        '''
        return list([(key, self.__values[key].value) for key in self.__values])


    def attach_attr_handler(self, attribute_handler):
        '''
        Attach the attribute handler from the session

        Many operation on attribute values require that the AttributeHandler class
        to transform values.  Because ETL components generate records and we don't
        want to make it any more difficult than necessary to create records, we
        attach the attribute handler the first time it's needed (on freeze())

        :param attribute_handler: EtlAttributeHandler
        '''
        self.__attr_handler = attribute_handler


    def _assert_has_handler(self):
        if self.__attr_handler is None:
            raise Exception("Attribute handler not present yet")


    def freeze(self):
        '''
        Freeze and record to prevent accidental updates

        :param attr_handler: EtlAttributeHandler
        '''
        if self.frozen:
            return

        self._assert_has_handler()

        if self.__serial is None:
            self.__serial = EtlSerial()

        for key, value in self.__values.items():
            self.__values[key] = self.__attr_handler.freeze(value)

        self.__frozen = True


    # -- Record Associations --------------------------------------------------

    def copy(self, rectype=None):
        '''
        Make a copy of the record that can be updated

        :param rectype: Change record type
        :return: EtlRecord
        '''
        self._assert_has_handler()
        if self.frozen:

            # Thaw values so that they can be changed

            values = OrderedDict()
            for key in self.__values:
                try:
                    values[key] = self.__attr_handler.thaw(self.__values[key])
                except Exception as e:
                    raise Exception("Failed to thaw %s value '%s': %s" % (
                        key,
                        self.__attr_handler.repr_value(self.__values[key]),
                        str(e)))

            return EtlRecord(
                record_type = rectype or self.record_type,
                serial = EtlSerial(),
                **values)

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


    def repr_attrs(self):
        '''
        Create string representaiton of field values for user readability

        :return: key, value pairs for each attribute
        '''
        self._assert_has_handler()
        for key, value in self.__values.items():
            yield key, self.__attr_handler.repr_value(value)

            # try:
            #     if value is None:
            #         value = 'none'
            #
            #     elif value.__class__ is datetime:
            #         value = value.strftime("%Y-%m-%d %H:%M:%S")
            #
            #     elif value.__class__ is date:
            #         return value.strftime("%Y-%m-%d")
            #
            #     else:
            #         value = pformat(value).strip("'")
            # except Exception as e:
            #     value = "Failed to represent value: %s" % (str(e))
            #
            # yield key, value


    def format(self, width=80, header=True, border=True):
        '''
        Create a nice, multiline, printable representation of the record

        :param width: Max width to use for formatting record
        :param header: Whether to include record header (else, just values)
        :param border: Whether to wrap record in a boarder
        :return: str
        '''
        rtn = list()

        attrs = list(self.repr_attrs())

        max_key_len = max([len(t[0]) for t in attrs])

        max_value_len = width - max_key_len
        if border:
            max_value_len -= 7
        max_value_len = min((max_value_len, max([len(t[1]) for t in attrs])))

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


    @property
    def attr_names(self):
        return self.__values.keys()
    def keys(self):
        return self.attr_name



    def items(self):
        return self.__values.items()


    @property
    def values(self):
        return tuple(self.items())


    @property
    def frozen(self):
        return self.__frozen


    def assert_not_frozen(self):
        if self.__frozen:
            raise EtlRecordFrozen()


    def __eq__(self, record):
        raise NotImplementedError("TODO")


