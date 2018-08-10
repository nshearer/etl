
from .. import EtlComponent, EtlInput, EtlOutput

class MapComponent(EtlComponent):
    '''
    General use component to run calculations on attributes

    Each record received on source will have a set of mapping functions
    called to change attribute values or create new ones.

    General uses are:
      1) Converting the format or type of an attribute
      2) Changing the name of an attribute
      3) Calculating an attribute value fron other values
    '''

    source = EtlInput()
    output = EtlOutput()

    def __init__(self):
        super(MapComponent, self).__init__()

        self.__maps = list()            # (attr_name, calc_func)
        self.__output_rectype = None
        self.__remove_attrs = set()
        self.__move_attrs = dict()


    def set_rectype(self, calc):
        '''
        Set the output record type

        :param calc:
            Either a callable that will be passwed the source record and should
            return the record_type, or the str record type to set
        '''
        self.__output_rectype = calc


    def map(self, name, calc):
        '''
        Add a mapping to calculate the value of an attribute on the record

        As each record is received, each mapping will be run such as:
            rec[attr_name] = calc(input_record)

        :param name:
            Name of the attribute to set on the output record
        :param calc:
            Callable to calculate the value.  Will be passed the
            input record.
        '''

        if name in self.__maps:
            raise Exception("Attribute '%s' already mapped" % (name))

        self.__maps.append(name, calc)


    def run(self):

        for source_rec in self.source.all():

            rec = source_rec.copy()

            # Set record type
            if self.__output_rectype is not None:
                try:
                    rec.record_type = self.__output_rectype(source_rec)
                except ValueError:
                    rec.record_type = self.__output_rectype

            # Move records
            for from_name, to_name in self.__move_attrs.items():
                raise NotImplementedError()

            # Do mappings
            for name, calc in self.__maps:
                 rec[name] = calc(source_rec)

            # Remove records
            for name in self.__remove_attrs:
                raise NotImplementedError()

            self.output.output(rec)


    def remove_attr(self, name):
        '''Remove an attribute from the source record'''
        self.__remove_attrs.append(name)


    def rename_attr(self, name, to_name):
        '''Change the name of an attribute'''
        self.__move_attrs[to_name] = name