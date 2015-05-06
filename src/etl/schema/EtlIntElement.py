from EtlSchemaElement import EtlSchemaElement

class EtlIntElement(EtlSchemaElement):
    '''Integer values'''

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
        return int(value)
        
    
    def access_value(self, stored_value, is_frozen):
        '''Return the stored value to the user
        
        @param stored_value: The value returned by validate_value()
        @param is_frozen: Has the record been frozen
        @return: Value to work with in ETL
        '''
        return stored_value