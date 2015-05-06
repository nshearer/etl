from abc import ABCMeta, abstractmethod


class SchemaValidationError(Exception): pass


class EtlSchemaElement(object):
    '''A field within a record

    These classes assist the ETL process with knowing how to handle values.
    They can also be used to specify constraints on the data that will be
    used to identify records with errors.

    Each attribute in a schema that represents a field in the record is an
    instantiated EltSchemaElement object.  Values for fields are stored in the
    EtlRecord object.
    '''
    __metaclass__ = ABCMeta

    def is_schema_element(self):
        '''Marker to tell other ETL code that this is a schema element'''
        return True


    @property
    def element_type_code(self):
        return self.__class__.__name__


    # -- Setup Methods -------------------------------------------------------

    def __init__(self):
        self.header = None


    def set_header(self, value):
        '''Set the column header used when describing this field'''
        self.header = value
        return self
    
    
    # -- Value validation ----------------------------------------------------
    
    @abstractmethod
    def validate_and_set_value(self, value):
        '''Check that the value being assigned is valid.
        
        Optionally convert the value into another.  The returned value is what
        gets stored in the record.  Throw SchemaValidationError() if an error
        is encountered.
        
        @param value: Value being set for this fiel in on the record
        @return: Value to be stored on record
        '''
        
    
    @abstractmethod
    def access_value(self, stored_value, is_frozen):
        '''Return the stored value to the user
        
        @param stored_value: The value returned by validate_value()
        @param is_frozen: Has the record been frozen
        @return: Value to work with in ETL
        '''
        
        
    def get_none_value(self, is_frozen):
        '''Get value to return if no value has been set
        
        @param is_frozen: Has the record been frozen
        @return: Value to work with in ETL
        '''
        return None
        
    
    # -- Use Functions -------------------------------------------------------

    def __eq__(self, other):
        if other is None:
            return False
        
        return str(self) == str(other)
        

    def _get_attr_for_str(self):
        '''Get attributes to include in string representation'''
        return {'header': self.header}


    def __str__(self):
        attrs = self._get_attr_for_str()
        attrs = ', '.join(['%s: %s' % (item) for item in attrs.items()])
        return '%s(%s)' % (self.element_type_code, attrs)
