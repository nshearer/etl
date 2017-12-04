from EtlSchemaElement import EtlSchemaElement, SchemaValidationError

class EtlRecordElement(EtlSchemaElement):
    '''An element that stores a record'''

    def __init__(self, record_schema):
        '''Init 

        @param record_schema:
            The schema of the record that this 
        '''
        self.__schema = record_schema
        super(EtlRecordElement, self).__init__()


    @property
    def record_schema(self):
        return self.__schema


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
        
        # Make sure record has a serial (Sanity Check)
        try:
            value.serial
        except AttributeError:
            msg = "Value is not a record?  (doesn't have a serial"
            raise SchemaValidationError(msg)
        
        return value
        
    
    def access_value(self, stored_value, is_frozen):
        '''Return the stored value to the user
        
        @param stored_value: The value returned by validate_value()
        @param is_frozen: Has the record been frozen
        @return: Value to work with in ETL
        '''
        if stored_value is not None:
            if is_frozen:
                stored_value.freeze()
        
        return stored_value


    def _get_attr_for_str(self):
        '''Get attributes to include in string representation'''
        attrs = super(EtlListElement, self)._get_attr_for_str()
        attrs['schema'] = str(self.__schema)
        return attrs


    def __eq__(self, other):
        if super(EtlListElement, self).__eq__(other):
            if self.__schema == other.__schema:
                return True
            return False
        return False