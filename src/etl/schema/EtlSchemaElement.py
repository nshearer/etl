
class EtlSchemaElement(object):
    '''A field within a record

    These classes assist the ETL process with knowing how to handle values.
    They can also be used to specify constraints on the data that will be
    used to identify records with errors.

    Each attribute in a schema that represents a field in the record is an
    instantiated EltSchemaElement object.  Values for fields are stored in the
    EtlRecord object.
    '''

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


    # -- Use Functions -------------------------------------------------------

    def __eq__(self, other):
        if other is None:
            return False
        
        try:
            if self.element_type_code != other.element_type_code:
                return False
        except AttributeError:
            return False

        return True
        

    def _get_attr_for_str(self):
        '''Get attributes to include in string representation'''
        return {'header': self.header}


    def __str__(self):
        attrs = self._get_attr_for_str()
        attrs = ', '.join(['%s: %s' % (item) for item in attrs.items()])
        return '%s(%s)' % (self.element_type_code, attrs)
