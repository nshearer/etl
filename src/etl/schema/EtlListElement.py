from EtlSchemaElement import EtlSchemaElement


class EtlListElementWrapper(list):
    
    def __init__(self, item_element):
        super(EtlListElementWrapper, self).__init__()
        self._is_frozen = False
        self._item_element = item_element
        
    def freeze(self):
        if self._is_frozen:
            return
        self._is_frozen = True
        for i, value in enumerate(self):
            self[i] = self._item_element.freeze_value(value)
    
    def assert_not_frozen(self):
        if self.__frozen:
            raise EtlRecordFrozen()

    def append(self, value):
        self.assert_not_frozen()
        value = self._item_element.validate_and_set_value(value)
        list.append(self, value)

    def extend(self, values):
        self.assert_not_frozen()
        for value in values:
            self.append(value)
        
    def __setitem__(self, index, value):
        self.assert_not_frozen()
        value = self._item_element.validate_and_set_value(value)
        list.__setitem__(self, index, value)
        


class EtlListElement(EtlSchemaElement):
    '''An element that stores a list of items'''

    def __init__(self, item_element):
        '''Init 

        @param item_element:
            An EtlSchemaElement for the items of the list
        '''
        self.__item_element = item_element
        super(EtlListElement, self).__init__()


    @property
    def item_element(self):
        return self.__item_element


    def _get_attr_for_str(self):
        '''Get attributes to include in string representation'''
        attrs = super(EtlListElement, self)._get_attr_for_str()
        attrs['item_element'] = str(self.__item_element)
        return attrs


    def __eq__(self, other):
        if super(EtlListElement, self).__eq__(other):
            if self.item_element == other.item_element:
                return True
            return False
        return False
    
    
    def validate_and_set_value(self, value):
        '''Check that the value being assigned is valid.
        
        Optionally convert the value into another.  The returned value is what
        gets stored in the record.  Throw SchemaValidationError() if an error
        is encountered.
        
        @param value: Value being set for this fiel in on the record
        @return: Value to be stored on record
        '''
        if value is None:
            return None
        
        # If already wrapped, use our class
        if value.__class__ is EtlListElementWrapper:
            return value            
        
        # Else, convert iterable to EtlListElementWrapper
        stored = EtlListElementWrapper(self.item_element)
        for item in value:
            stored.append(value)
        
    
    def access_value(self, stored_value, is_frozen):
        '''Return the stored value to the user
        
        @param stored_value: The value returned by validate_value()
        @param is_frozen: Has the record been frozen
        @return: Value to work with in ETL
        '''
        return stored_value
        
        
    def get_none_value(self, is_frozen):
        '''Get value to return if no value has been set
        
        @param is_frozen: Has the record been frozen
        @return: Value to work with in ETL
        '''
        return None


    def freeze_value(self, stored_value):
        '''Cascade the freeze action down to the values
        
        @param stored_value: The value returned by validate_value()
        @return: Value to store in values
        '''        
        if stored_value is not None:
            stored_value.freeze()
        return stored_value
        