from abc import abstractmethod
import logging
import json
from datetime import datetime, date, timedelta
from decimal import Decimal

from .exceptions import NoAttributeValueHandler

from frozendict import frozendict


class AttrNotACollection(Exception): pass
class FreezeFailed(Exception): pass
class StoreValueFailed(Exception): pass

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
    def is_frozen_attr_value(self):
        return False


class FrozenAttributeValue(AttributeValue):
    '''Encapsulates the immutable value of an attribute'''

    def __init__(self, value, orig_value_clsname):
        '''
        :param value: The frozen value to store
        :param orig_value_clsname: The origional value class name
        '''
        super(FrozenAttributeValue, self).__init__(value)
        self.__orig_value_clsname = orig_value_clsname


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
        return True

    @property
    def is_frozen_attr_value(self):
        return True

    @property
    def is_frozen_collection(self):
        return False


    def freeze_collection_metadata(self):
        '''Generate metadata for thawing this item if it's embedded in a collection'''
        return {
            'type': 'single',
            'orig_class': self.orig_value_clsname,
        }


class NotFrozenAttributeValue(FrozenAttributeValue):
    '''A value that we failed to freeze'''


    def __init__(self, value):
        super(NotFrozenAttributeValue, self).__init__(value, value.__class__.__name__)

    @property
    def freeze_successful(self):
        return False



class FrozenAttributeCollection(FrozenAttributeValue):
    '''Encapsulates the immutable value of an attribute that is a collection'''

    def __init__(self, value, orig_value_clsname, freeze_success=True, item_metadata=None):
        '''
        :param value:
            The frozen value to store
        :param orig_value_clsname:
            The origional value class name
        :param item_metadata:
            Dictionary of item cfreeze metadata collected from freeze_collection_metadata()
        '''

        super(FrozenAttributeCollection, self).__init__(
            value=value,
            orig_value_clsname=orig_value_clsname,
        )

        self.__item_metadata = frozendict(item_metadata)


    def __repr__(self):
        return 'FrozenAttributeCollection(%s)' % (repr(self.value))


    @property
    def is_frozen_collection(self):
        return True


    def freeze_collection_metadata(self):
        '''Generate metadata for thawing this item if it's embedded in a collection'''
        return {
            'type': 'collection',
            'orig_class': self.orig_value_clsname,
            'item_metadata': self.__item_metadata,
        }


    def wrap_containted_item(self, key, frozen_value):
        '''
        Wrap a member of this collection in an appropiate FrozenAttributeValue class to thaw

        :param key: Key of the collection for this item
        :param frozen_value: The raw frozen value
        :return: FrozenAttributeValue
        '''

        try:
            metadata = self.__item_metadata[key]
        except KeyError:
            raise KeyError("\n".join((
                "No item metadata is stored for collecting member '%s'" % (key),
                "This probably means that EtlAttributeHandler.list_%s_members()" % (self.orig_value_clsname),
                " returned a different set of members")))

        if metadata['type'] == 'single':
            return FrozenAttributeValue(
                value = frozen_value,
                orig_value_clsname = metadata['orig_class'],
            )
        elif metadata['type'] == 'collection':
            return FrozenAttributeCollection(
                value = frozen_value,
                orig_value_clsname = metadata['orig_class'],
                item_metadata = metadata['item_metadata'],
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

    NON_COL_TYPES = {
        str,
        bytes,
        int,
        date,
        float,
        datetime,
        None.__class__,
        Decimal,
    }



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

        try:

            # Assume is collection
            try:

                # Freeze all of the contained values of the collection
                frozen_items = [(key, self.freeze(AttributeValue(item)))
                                for (key, item)
                                in self._list_collection_members(value.value)]

                # Check that freezing all children succeeded
                # If freezing of a single item failed, fail whole freeze
                if not min([t[1].freeze_successful for t in frozen_items]):
                    return NotFrozenAttributeValue(value)

                # Now, strip FrozenAttributeValue off the value
                item_freeze_data = {key: item.freeze_collection_metadata()
                                    for (key, item) in frozen_items}
                frozen_items = [(key, item.value) for (key, item) in frozen_items]

                # Freeze collection
                frozen_raw_value = self._freeze_raw(value.value, frozen_members = frozen_items)

                # return frozen collection
                return FrozenAttributeCollection(
                    value = frozen_raw_value,
                    orig_value_clsname = value.value.__class__.__name__,
                    item_metadata = item_freeze_data,
                )

            except AttrNotACollection:

                frozen_raw_value = self._freeze_raw(value.value)
                return FrozenAttributeValue(
                    value = frozen_raw_value,
                    orig_value_clsname = value.value.__class__.__name__,
                )

        except NoAttributeValueHandler as e:
            if self.freeze_required:
                raise e
            else:
                return NotFrozenAttributeValue(value)


    def _list_collection_members(self, value):
        '''
        List all of the items in a collection with their keys

        This handler supports the handling of values that are collections with
        items which can also be nested collections.  The only requirement is that
        each item in the collection have a unique key.

        This method calls list_*_members(value) to get a listing of the keys
        and members returned as tuples (key, value).

        If the value passed to this method is not a collection, then
        AttrNotACollection will be raised.

        :param value: Any collection type
        :return: generatory of (key, item)
        '''

        # Silently return if not a collection type
        if value.__class__ in self.NON_COL_TYPES:
            raise AttrNotACollection()

        # Determine handler method to freeze
        try:
            list_call_name = 'list_%s_members' % (value.__class__.__name__)
            list_call = getattr(self, list_call_name)
            return list_call(value)
        except AttributeError:
            # Assume not a collection
            raise AttrNotACollection()


    def _calc_value_clsname(self, value):
        '''
        Which class name should be used when calling methods in the attribute handler

        :param value: Unwrapped value
        '''
        return value.__class__.__name__.lower()


    def _freeze_raw(self, value, frozen_members=None):
        '''
        Determine the handler method and call it to make the value immutable

        :param value:
            Value to be frozen (as it was given to the record, not wrapped)
        :param frozen_members:
            If this is a collection, then list of (key, value) pairs of already
            frozen contained items
        :return:
            raw immutable version of value
        '''

        # Determine handler method to freeze
        try:
            freeze_calc_name = 'freeze_%s' % (self._calc_value_clsname(value))
            freeze_calc = getattr(self, freeze_calc_name)
        except AttributeError:

            # Ignore immutable types
            if value.__class__ in self.IMMUTABLE_TYPES:
                return value

            msg = "Missing session.attribute_handler.%s())" % (freeze_calc_name)
            raise NoAttributeValueHandler(msg)

        # Perform freeze
        if frozen_members is None:
            return freeze_calc(value)
        else:
            return freeze_calc(value, frozen_members)



    # == Thaw ================================================================

    def thaw(self, value):
        '''
        Make value mutable again (reversing freeze())

        FrozenAttributeValue -> AttributeValue

        :param value: FrozenAttributeValue containing value to be thawed
        :return: FrozenAttributeValue
        '''

        # If freeze_value() couldn't find a handler to freeze, then
        # pass same value back
        if not value.freeze_successful:
            return AttributeValue(value=value.value)

        # Thaw non-collection attributes
        if not value.is_frozen_collection:

            return AttributeValue(
                value = self._thaw_raw(value.value, value.orig_value_clsname)
            )

        # Thaw collection values
        else:

            # Thaw each of the contained items
            frozen_items = list(self._list_collection_members(value.value))
            thawed_items = list()
            for key, item_value in frozen_items:

                # Build FrozenAttributeValue to wrap this item so we can just
                # call thaw()
                wrapped_item = value.wrap_containted_item(key, item_value)
                thawed_items.append((key, self.thaw(wrapped_item).value))

            # Thaw collection
            return AttributeValue(
                value = self._thaw_raw(
                    value = value.value,
                    orig_cls_name = value.orig_value_clsname,
                    thawed_members = thawed_items)
            )

            raise NotImplementedError()


    def _thaw_raw(self, value, orig_cls_name, thawed_members=None):
        '''
        Find and run appropriate thaw_*() method

        :param value: "Raw" value not wrapped by AttributeValue
        :param orig_cls_name: The original classname of the value
        :param thawed_members:
            If the value is a collection, then the items already thawed as a list of
            (key, value)
        :return: raw value (hopefully the original value passed to freeze())
        '''

        # Find handler to thaw
        try:
            thaw_calc_name = 'thaw_%s' % (orig_cls_name)
            thaw_calc = getattr(self, thaw_calc_name)
        except AttributeError:

            # Ignore immutable types
            if value.__class__ in self.IMMUTABLE_TYPES:
                return value

            # Handle Error
            msg = "Missing session.attribute_handler.%s())" % (thaw_calc_name)
            raise NoAttributeValueHandler(msg)

        # Perform thaw calc
        if thawed_members is None:
            return thaw_calc(value)
        else:
            return thaw_calc(value, thawed_members)


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


    # -- list --

    def list_list_members(self, value):
        return enumerate(value)

    def list_tuple_members(self, value):
        return enumerate(value)

    def freeze_list(self, value, frozen_members):
        return tuple([item for (key, item) in sorted(frozen_members)])

    def thaw_list(self, value, thawed_members):
        return [item for (key, item) in sorted(thawed_members)]


    # -- dict --

    def list_dict_members(self, value):
        return value.items()

    def list_frozendict_members(self, value):
        return self.list_dict_members(value)

    def freeze_dict(self, value, frozen_members):
        return frozendict({key: item for (key, item) in frozen_members})

    def thaw_dict(self, value, thawed_members):
        return {key: item for (key, item) in thawed_members}



    # == Store ================================================================

    def store(self, value):
        '''
        Generate a string to store the value of this attribute to disk

        FrozenAttributeValue -> string

        :param value: FrozenAttributeValue containing value to be stored
        :return: value ready for pickle
        '''

        # Check already frozen
        if not value.is_frozen_attr_value:
            raise Exception("Can't call store() on %s" % (value.__class__.__name__))

        meta = value.freeze_collection_metadata()
        meta['value'] = self._prepickle_raw_value(value.value)

        try:
            return json.dumps(meta)
        except Exception as e:
            raise StoreValueFailed("Failed to store attibute value %s: %s: %s" % (
                repr(value.value), e.__class__.__name__, str(e)))


    SKIP_PREPICKLE = {
        str,
        bytes,
        int,
        float,
        None.__class__,
    }


    def _prepickle_raw_value(self, value):
        '''
        Some values won't pickle easily.  So, this handler calls prepickle to allow types to be converted

        Note: since we don't know if contained

        :param value: Raw value to be stored
        :return: raw value ready to be pickled
        '''

        # Skip?
        if value.__class__ in self.SKIP_PREPICKLE:
            return value

        # Find prepickle method
        try:
            prepickle_calc_name = 'prepickle_%s' % (self._calc_value_clsname(value))
            prepickle_calc = getattr(self, prepickle_calc_name)
        except AttributeError:
            msg = "Missing session.attribute_handler.%s())" % (prepickle_calc_name)
            raise NoAttributeValueHandler(msg)

        # For collection, recursivly prepickle
        try:
            prepickeled_items = dict()
            for key, item in self._list_collection_members(value):
                prepickeled_items[key] = self._prepickle_raw_value(item)

            # Pickle collection
            return prepickle_calc(value, prepickeled_items)

        # For non-collections, just pre-pickle
        except AttrNotACollection:
            return prepickle_calc(value)


    DATE_FORMAT = "%c"

    def prepickle_date(self, value):
        return value.strftime(self.DATE_FORMAT)

    def prepickle_datetime(self, value):
        return value.strftime(self.DATE_FORMAT)

    def prepickle_decimal(self, value):
        return str(value)

    def prepickle_tuple(self, value, items):
        return [items[i] for i in sorted(items.keys())]

