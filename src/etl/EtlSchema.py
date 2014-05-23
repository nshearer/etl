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
        self.__fields = dict()
        self.__field_order = list()
        
        
    def add_field(self, name, desc=None, header=None, type_hint='str'):
        '''Add a field to the schema
        
        @param name: Name of the field and field key in the records
        @param desc: Long description of the field
        @param header: Header to use when dumping records to a file
        '''
        if header is None:
            header = name
        if self.__fields.has_key(name):
            raise IndexError("Field %s already exists in schema" % (name))
        self.__fields[name] = (header, desc, type_hint)
        self.__field_order.append(name)
        
        
    def remove_field(self, name):
        '''Remove a field from the schema'''
        if not self.__fields.has_key(name):
            raise IndexError("Field %s not in schema" % (name))
        del self.__fields[name]
        self.__field_order.remove(name)
        
        
    def check_record_struct(self, record):
        '''Check to see if the record matches this schema'''
        errors = list()
        expected_fields = set(self.__fields.keys())
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
    
    
    def copy_field(self, from_schema, name):
        '''Copy a field from another schema to this schema'''
        # Get field from schema
        if not from_schema.__fields.has_key(name):
            raise IndexError("Field %s not in schema" % (name))
        header, desc, type_hint = from_schema.__fields[name]
        
        # Add to this schema
        self.add_field(name, desc, header, type_hint)
    
    
    def clone(self):
        n = EtlSchema()
        n.__fields = self.__fields.copy()
        n.__field_order = self.__field_order[:]
        return n
        
        
    def list_field_names(self):
        return self.__field_order[:]
        
        
    def list_fields(self):
        rtn = list()
        for name in self.__field_order:
            rtn.append({
                'name':     name,
                'header':   self.__fields[name][0],
                'desc':     self.__fields[name][1],
                'type':     self.__fields[name][2],
                })
        return rtn
    
    
#     def format_field_value_for_excel(self, field_name, value):
#         '''Format the given value to be stored in Excel'''
#         return value
#     
#     
#     def format_field_value_for_csv(self, field_name, value):
#         '''Format the given value to be stored in Excel'''
# #        if self.__fields[field_name][2] == self.STRING:
# #            return "'%s" % (value)
#         return value
    
    
    def __eq__(self, schema):
        if schema is None:
            return False
        
        try:
            if self.__field_order == schema.__field_order:
                if self.__fields == schema.__fields:
                    return True
        except AttributeError:
            return False
        return False

        