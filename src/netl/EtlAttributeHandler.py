from abc import abstractmethod
import logging
from datetime import datetime, date, timedelta
from decimal import Decimal

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
    def is_frozen_attr_value(self):
        return False


class FrozenAttributeValue(AttributeValue):
    '''Encapsulates the immutable value of an attribute'''

    def __init__(self, value, orig_value_clsname, freeze_metadata=None, freeze_success=True):
        '''
        :param value: The frozen value to store
        :param orig_value_clsname: The origional value class name
        :param freeze_success: Was the value successfully frozen (made into
            an immutable type) by the handler.
        :param freeze_metadata:
            Extra metadata regarding the freezing of the data to be used
            when thawing.
        '''
        super(FrozenAttributeValue, self).__init__(value)
        self.__orig_value_clsname = orig_value_clsname
        self.__freeze_success = freeze_success
        self.__freeze_metadata = freeze_metadata

        try:
            if self.__freeze_metadata is not None:
                self.__freeze_metadata = frozendict(self.__freeze_metadata)
        except:
            raise Exception("Make sure freeze_metadata is a dict")


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
    def is_frozen_attr_value(self):
        return True

    @property
    def metadata(self):
        return self.__freeze_metadata


class FrozenCollectionFactory:
    '''
    Helper class for freezing collection attributes

    Feezing collections is a multi-step process where each item will
    also be frozen.  This class is provided to help streamline the
    process.

    Basic usage is:

        def freeze_list(list_value):

            fcf = Instantiate FrozenCollectionFactory()

            # Freeze the items
            fcf.value = list()
            for i, item in enumerate(list_value):
                frozen.append(fcf.add_item(
                    EtlAttributeHandler.freeze(unwrapped=item),
                    item_key = i))

            # Freeze the list data structure
            fcf.value = tuple(fcf.value)

            return fcf

    '''


    def __init__(self):
        self.__item_class = dict()
        self.__item_metadata = dict()
        self.value = None


    @property
    def is_frozen_collection_factory(self):
        return True


    def add_item(self, value, item_key):
        '''
        Add an item (member of the collection)

        :param value:
            A FrozenAttributeValue returned for the item from
            EtlAttributeHandler.freeze_value()
        :param item_key:
            A key to uniquly identify this item in the collection
        :return: mixed
            The value without the FrozenAttributeValue wrapper
            suitable to be added to the frozen collection value.
        '''

        if item_key in self.__item_class:
            raise KeyError("Item key used more than once: " + str(item_key))

        # Strip FrozenAttributeValue
        self.__item_class[item_key] = value.orig_value_clsname
        self.__item_metadata[item_key] = value.metadata

        return value.value


    def __call__(self, collection_cls_name):
        '''
        Perform creation of the collection FrozenAttributeValue()

        :param collection_cls_name:
            The lass name of the original collection to determine which
            thaw_*() method to call to thaw this back out
        :return: FrozenAttributeValue
        '''

        return FrozenAttributeValue(
            self.value,
            collection_cls_name,
            freeze_metadata = {
                'item_classes': frozendict(self.__item_metadata),
                'item_metadata': frozendict(self.__item_metadata),
            }
        )


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
        bytes,
        int,
        date,
        float,
        datetime,
        None.__class__,
        Decimal,
    )


    # == Primary methods ======================================================

    def freeze(self, value):
        '''
        Make value immutable

        AttributeValue -> FrozenAttributeValue

        :param value: AttributeValue containing value to be frozen
        :return: FrozenAttributeValue
        '''

        # Check already frozen
        if value.is_frozen_attr_value:
            return value

        # Determine handler method to freeze
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


    # == Collection freezing =================================================