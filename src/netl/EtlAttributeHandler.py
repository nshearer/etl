import logging
from datetime import datetime, date, timedelta

from .exceptions import NoFreezeFunction, ValueFreezeFailed

from frozendict import frozendict


class AttributeValue:
    '''Encapsulates the value of an attribute'''

    def __init__(self, value):
        self.__value = value
        self.__orig_class = value.__class__


class FrozenAttributeValue(AttributeValue):
    '''Encapsulates the immutable value of an attribute'''


class StoredAttributeValue(FrozenAttributeValue):
    '''Value stored to disk (json)'''


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


    def add_freezer_func(self, cls, func):
        '''
        Add a custom function to freeze values

        Will pass (value, freezer) to function.  If cls represents a collection,
        then custom freezer should recursivly call freezer.freeze(value) on all
        items.

        :param cls: Class this function should be run on
        :param func: Callback to freeze value
        :return:
        '''
        self._assert_not_started()
        self.__freeze_functions[cls] = func


    def suppress_freeze(self, cls):
        '''
        Instruct freezer not to touch this class when freezing values

        :param cls: Class to not freeze
        '''
        self.add_freezer_func(cls, func=None)


    IMMUTABLE_TYPES = (bool, int, float, str, datetime, date, timedelta)
    IMMUTABLE_COLLECTIONS = (tuple, frozendict, frozenset)


    def _check_already_frozen(self, value):

        if value is None:
            return True

        if value.__class__ in self.IMMUTABLE_TYPES:
            return True

        # Check for already immutable collections
        if value.__class__ in (tuple, frozenset):
            for item in value:
                if not self._check_already_frozen(item):
                    return False
            return True

        elif value.__class__ is frozendict:
            for item in value.values():
                if not self._check_already_frozen(item):
                    return False
            return True

        return False


    def freeze(self, value):
        '''
        Create an immutable version of the value
        '''

        # Check for custom freezer functions
        try:
            func = self.__freeze_functions[value.__class__]

            if func is None:
                return value

            try:
                return func(value, self)
            except Exception as e:
                msg = "%s custom freezer func failed for value %s: %s" % (
                      value.__class__.__name__, repr(value), str(e))
                if self.freeze_required:
                    raise ValueFreezeFailed(msg)

        except KeyError:
            pass


        # Check already immutable
        if self._check_already_frozen(value):
            return value

        # Standard freezers
        if value.__class__ in (list, tuple):
            return tuple([self.freeze(v) for v in value])

        elif value.__class__ in (set, frozenset):
            return frozenset([self.freeze(v) for v in value])

        elif value.__class__ in (dict, frozendict):
            return frozendict({k: self.freeze(v) for (k, v) in value.items()})

        # Didn't find a method to freeze this value
        msg = "No freezer function found for class %s" % (value.__class__.__name__)
        if self.freeze_required:
            raise NoFreezeFunction(msg)


