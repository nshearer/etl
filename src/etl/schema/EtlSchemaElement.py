
class EtlSchemaElement(object):
    '''A field within a record

    These classes assist the ETL process with knowing how to handle values.
    They can also be used to specify constraints on the data that will be
    used to identify records with errors.

    Each attribute in a schema that represents a field in the record is an
    instantiated EltSchemaElement object.  Values for fields are stored in the
    EtlRecord object.
    '''

    def __init__(self, header=None):
    	'''Init

    	@param header:
    		The header string displayed for this field when producing reports
    	'''
    	self.header = header


    def is_schema_element(self):
        '''Marker to tell other ETL code that this is a schema element'''
        return True


    @property
    def element_type_code(self):
        return self.__class__.__name__


    def __eq__(self, other):
        if other is None:
            return False
        
        try:
            if self.element_type_code != other.element_type_code:
                return False
        except AttributeError:
            return False

        return True
        
