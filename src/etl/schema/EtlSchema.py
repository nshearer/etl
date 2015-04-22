'''
Created on Dec 27, 2012

@author: nshearer
'''

class EtlSchema(object):
    '''Describes the structure of a record'''
    
    STRING = 'str'
    INT = 'int'
    FLOAT = 'int'
    DATE = 'date'
    BOOL = 'bool'
    
    def __init__(self):
        self.__added_fields = dict()
        self.__added_field_order = list()# Order is not preserved for non-added
        
        
    def add_field(self, name, element_class):
        '''Add a field to the schema
        
        @param name: Name of the field and field key in the records
        @param desc: Long description of the field
        @param header: Header to use when dumping records to a file
        '''
        if name in self.etl_list_fields():
            raise IndexError("Field %s already exists in schema" % (name))
        self.__added_fields[name] = element_class
        self.__added_field_order.append(name)
        
        
    # def remove_field(self, name):
    #     '''Remove a field from the schema'''
    #     if not self.__added_fields.has_key(name):
    #         raise IndexError("Field %s not in schema" % (name))
    #     del self.__added_fields[name]
    #     self.__added_field_order.remove(name)
        
        
    def etl_check_record_struct(self, record):
        '''Check to see if the record matches this schema'''
        errors = list()
        expected_fields = set(self.etl_list_field_names())
        rec_fields = set(record.field_names())
        
        # Check for missing fields
        for name in expected_fields - rec_fields:
            msg = "Missing required field '%s'" % (name)
            errors.append(record.create_msg(msg))

        # Check for extra fields
        for name in rec_fields - expected_fields:
            msg = "Unknown field '%s'" % (name)
            errors.append(record.create_msg(msg))
        
        if len(errors) == 0:
            return None
        return errors
    
    
    # def copy_field(self, from_schema, name):
    #     '''Copy a field from another schema to this schema'''
    #     # Get field from schema
    #     if not from_schema.__added_fields.has_key(name):
    #         raise IndexError("Field %s not in schema" % (name))
    #     header, desc, type_hint = from_schema.__added_fields[name]
        
    #     # Add to this schema
    #     self.add_field(name, desc, header, type_hint)
    
    
    # def clone(self):
    #     n = EtlSchema()
    #     n.__added_fields = self.__added_fields.copy()
    #     n.__added_field_order = self.__added_field_order[:]
    #     return n
        
        
    def etl_list_field_names(self):
        for attr_name, attr_eclass in self.etl_list_fields():
            yield attr_name

        
        
    def etl_list_fields(self):
        for name in dir(self):
            element = self.etl_get_field(name)
            if element is not None:
                yield name, element
        for name in self.__added_field_order:
            yield name, self.__added_fields[name]


    def etl_get_field(self, name):
        '''Get an element (field) of the schema by name'''
        if name[0] != '_':
            if hasattr(self, name):
                attr = getattr(self, name)
                if self._check_obj_is_element(attr):
                    return attr
            if self.__added_fields.has_key(name):
                return self.__added_fields[name]
            
        
    def _check_obj_is_element(self, obj):
        '''Check to see if the given object is an element definition'''
        if hasattr(obj, 'is_schema_element'):
            if obj.is_schema_element:
                return True
        return False


    def __getitem__(self, name):
        return self.etl_get_field(name)
    
#     def format_field_value_for_excel(self, field_name, value):
#         '''Format the given value to be stored in Excel'''
#         return value
#     
#     
#     def format_field_value_for_csv(self, field_name, value):
#         '''Format the given value to be stored in Excel'''
# #        if self.__added_fields[field_name][2] == self.STRING:
# #            return "'%s" % (value)
#         return value
    
    
    def __eq__(self, schema):
        if schema is None:
            return False
        
        try:
            if self.__added_field_order == schema.__added_field_order:
                if self.__added_fields == schema.__added_fields:
                    return True
        except AttributeError:
            return False
        return False

        