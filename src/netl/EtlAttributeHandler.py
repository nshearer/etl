import logging
from datetime import datetime, date, timedelta

from .exceptions import NoAttributeValueHandler

from frozendict import frozendict


class AttributeValue:
    '''Encapsulates the value of an attribute'''

    def __init__(self, value):
        self.__value = value

    @property
    def value(self):
        return self.__value

    def set_value(self, value):
        self.__value = value

    def __repr__(self):
        return repr(self.__value)


class FrozenAttributeValue(AttributeValue):
    '''Encapsulates the immutable value of an attribute'''

    def __init__(self, value, orig_value_clsname):
        super(FrozenAttributeValue, self).__init__(value)
        self.__orig_value_clsname = orig_value_clsname

    def set_value(self, value):
        raise Exception("Value is frozen")

    @property
    def orig_value_clsname(self):
        return self.__orig_value_clsname

    def __repr__(self):
        return 'FrozenAttributeValue(%s)' % (repr(self.value))



class EtlAttributeHandler:
    '''
    Contains the logic to work with record values
    '''

    def __init__(self):
        self.__freeze_functions = dict()
        self.__started = False
        self.logger = None # Set by EtlSession

        # If True, will cause Exception if we can't freeze a value
        self.freeze_required = True


    def mark_started(self):
        '''Tell object that ETL has started to prevent updates'''
        self.__started = True


    def _assert_not_started(self):
        if self.__started:
            raise Exception("This method can't be used once ETL is started")


    # == Primary methods ======================================================

    def freeze(self, value):
        '''
        Make value immutable

        AttributeValue -> FrozenAttributeValue

        :param value: AttributeValue containing value to be frozen
        :return: FrozenAttributeValue
        '''

        try:
            freeze_member_name = 'freeze_%s' % (value.__class__.__name__.lower())
            freeze_calc = getattr(self, freeze_member_name)
        except AttributeError:
            if self.freeze_required:
                msg = "No freezer function found for class %s\n(missing session.attribute_handler.%s())" % (
                    value.__class__.__name__, freeze_member_name)
                raise NoAttributeValueHandler(msg)

        return FrozenAttributeValue(
            value = freeze_calc(value),
            orig_value_clsname = value.__class__.__name__,
        )

    def thaw(self, value):
        '''
        Make value mutable again (reversing freeze())

        FrozenAttributeValue -> AttributeValue

        :param value: FrozenAttributeValue containing value to be thawed
        :return: FrozenAttributeValue
        '''

        try:
            thaw_member_name = 'thaw_%s' % (value.orig_value_clsname)
            thaw_calc = getattr(self, thaw_member_name)
        except AttributeError:
            if self.freeze_required:
                msg = "No thaw function found for class %s\n(missing session.attribute_handler.%s())" % (
                    value.__class__.__name__, thaw_member_name)
                raise NoAttributeValueHandler(msg)

        return AttributeValue(
            value = thaw_calc(value)
        )

    def thaw(self, value):
        '''
        Describe the value to the user

        :param value: AttributeValue or FrozenAttributeValue
        :return: str
        '''

        try:

            try:
                clsname = value.orig_value_clsname
            except:
                clsname = value.__class__.__name__
            repr_member_name = 'repr_%s' % (clsname.lower())
            repr_calc = getattr(self, repr_member_name)
        except AttributeError:
            if self.freeze_required:
                return "No representation function found for class %s\n(missing session.attribute_handler.%s())" % (
                    value.__class__.__name__, repr_member_name)

        return str(repr_calc(value))


    # -- str ------------------------------------------------------------------

    def freeze_str(self, value):
        '''Strings are immutable.  Just return'''
        return value



    # def add_freezer_func(self, cls, func):
    #     '''
    #     Add a custom function to freeze values
    #
    #     Will pass (value, freezer) to function.  If cls represents a collection,
    #     then custom freezer should recursivly call freezer.freeze(value) on all
    #     items.
    #
    #     :param cls: Class this function should be run on
    #     :param func: Callback to freeze value
    #     :return:
    #     '''
    #     self._assert_not_started()
    #     self.__freeze_functions[cls] = func
    #
    #
    # def suppress_freeze(self, cls):
    #     '''
    #     Instruct freezer not to touch this class when freezing values
    #
    #     :param cls: Class to not freeze
    #     '''
    #     self.add_freezer_func(cls, func=None)
    #
    #
    # IMMUTABLE_TYPES = (bool, int, float, str, datetime, date, timedelta)
    # IMMUTABLE_COLLECTIONS = (tuple, frozendict, frozenset)
    #
    #
    # def _check_already_frozen(self, value):
    #
    #     if value is None:
    #         return True
    #
    #     if value.__class__ in self.IMMUTABLE_TYPES:
    #         return True
    #
    #     # Check for already immutable collections
    #     if value.__class__ in (tuple, frozenset):
    #         for item in value:
    #             if not self._check_already_frozen(item):
    #                 return False
    #         return True
    #
    #     elif value.__class__ is frozendict:
    #         for item in value.values():
    #             if not self._check_already_frozen(item):
    #                 return False
    #         return True
    #
    #     return False
    #
    #
    # def freeze(self, value):
    #     '''
    #     Create an immutable version of the value
    #     '''
    #
    #     # Check for custom freezer functions
    #     try:
    #         func = self.__freeze_functions[value.__class__]
    #
    #         if func is None:
    #             return value
    #
    #         try:
    #             return func(value, self)
    #         except Exception as e:
    #             msg = "%s custom freezer func failed for value %s: %s" % (
    #                   value.__class__.__name__, repr(value), str(e))
    #             if self.freeze_required:
    #                 raise ValueFreezeFailed(msg)
    #
    #     except KeyError:
    #         pass
    #
    #
    #     # Check already immutable
    #     if self._check_already_frozen(value):
    #         return value
    #
    #     # Standard freezers
    #     if value.__class__ in (list, tuple):
    #         return tuple([self.freeze(v) for v in value])
    #
    #     elif value.__class__ in (set, frozenset):
    #         return frozenset([self.freeze(v) for v in value])
    #
    #     elif value.__class__ in (dict, frozendict):
    #         return frozendict({k: self.freeze(v) for (k, v) in value.items()})
    #
    #     # Didn't find a method to freeze this value
    #     msg = "No freezer function found for class %s" % (value.__class__.__name__)
    #     if self.freeze_required:
    #         raise NoFreezeFunction(msg)
    #
    #
