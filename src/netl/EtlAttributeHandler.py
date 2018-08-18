from abc import abstractmethod
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

    @property
    def class_name_for_handler(self):
        '''Which class name should be used when calling methods in the attribute handler'''
        return self.__value.__class__.__name__.lower()

    @property
    def is_frozen(self):
        return False


class FrozenAttributeValue(AttributeValue):
    '''Encapsulates the immutable value of an attribute'''

    def __init__(self, value, orig_value_clsname, freeze_success=True):
        '''
        :param value: The frozen value to store
        :param orig_value_clsname: The origional value class name
        :param freeze_success: Was the value successfully frozen (made into
                an immutable type) by the handler.
        '''
        super(FrozenAttributeValue, self).__init__(value)
        self.__orig_value_clsname = orig_value_clsname
        self.__freeze_success = freeze_success

    def set_value(self, value):
        raise Exception("Value is frozen")

    @property
    def orig_value_clsname(self):
        return self.__orig_value_clsname

    def __repr__(self):
        return 'FrozenAttributeValue(%s)' % (repr(self.value))

    @property
    def class_name_for_handler(self):
        '''Which class name should be used when calling methods in the attribute handler'''
        return self.orig_value_clsname.lower()

    @property
    def freeze_successful(self):
        return self.__freeze_success

    @property
    def is_frozen(self):
        return True


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


    IMMUTABLE_TYPES = (
        str,
        int,
        date,
        datetime,
        None
    )


    # == Primary methods ======================================================

    def freeze(self, value):
        '''
        Make value immutable

        AttributeValue -> FrozenAttributeValue

        :param value: AttributeValue containing value to be frozen
        :return: FrozenAttributeValue
        '''

        try:
            freeze_calc_name = 'freeze_%s' % (value.class_name_for_handler)
            freeze_calc = getattr(self, freeze_calc_name)
        except AttributeError:

            # Ignore immutable types
            if value.value.__class__ in self.IMMUTABLE_TYPES:
                return value

            # Handle Error
            if self.freeze_required:
                msg = "Missing session.attribute_handler.%s())" % (freeze_calc_name)
                raise NoAttributeValueHandler(msg)
            else:
                # Pass through original value
                return FrozenAttributeValue(
                    value = value.value,
                    orig_value_clsname = value.value.__class__.__name__,
                )

        # Perform freeze
        return FrozenAttributeValue(
            value = freeze_calc(value.value),
            orig_value_clsname = value.value.__class__.__name__,
        )


    def thaw(self, value):
        '''
        Make value mutable again (reversing freeze())

        FrozenAttributeValue -> AttributeValue

        :param value: FrozenAttributeValue containing value to be thawed
        :return: FrozenAttributeValue
        '''

        # If we get passed a non-frozen attribute, silently ignore
        if not value.is_frozen:
            return AttributeValue(value=value.value)

        # If freeze_value() couldn't find a handler to freeze, then
        # pass same value back
        if not value.freeze_successful:
            return AttributeValue(value=value.value)

        # Find handler to thaw
        try:
            thaw_calc_name = 'thaw_%s' % (value.class_name_for_handler)
            thaw_calc = getattr(self, thaw_calc_name)
        except AttributeError:

            # Ignore immutable types
            if value.value.__class__ in self.IMMUTABLE_TYPES:
                return value

            # Handle Error
            msg = "Missing session.attribute_handler.%s())" % (thaw_calc_name)
            raise NoAttributeValueHandler(msg)

        # Perform thaw calc
        return AttributeValue(
            value = thaw_calc(value.value)
        )

    def repr_value(self, value):
        '''
        Describe the value to the user

        :param value: AttributeValue or FrozenAttributeValue
        :return: str
        '''

        try:
            repr_calc_name = 'repr_%s' % (value.class_name_for_handler)
            repr_calc = getattr(self, repr_calc_name)
        except AttributeError:
            # No repr handler found
            return repr(value.value)

        return str(repr_calc(value.value))


    # # -- none -----------------------------------------------------------------
    #
    # # Strings are immutable.  Just pass through
    #
    # def freeze_nonetype(self, value):
    #     return value
    #
    # def thaw_nonetype(self, value):
    #     return value
    #
    #
    # # -- str ------------------------------------------------------------------
    #
    # # Strings are immutable.  Just pass through
    #
    # def freeze_str(self, value):
    #     return value
    #
    # def thaw_str(self, value):
    #     return value
    #
    #
    # # -- int ------------------------------------------------------------------
    #
    # # Ints are immutable.  Just pass through
    # # TODO: Make generice immutable handlers
    #
    # def freeze_int(self, value):
    #     return value
    #
    # def thaw_int(self, value):
    #     return value



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
