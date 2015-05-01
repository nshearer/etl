import re

from etl.EtlProcessor import EtlProcessor, EtlProcessorDataPort

class FieldFindReplace(EtlProcessor):
    '''Find and replace values in one or more fields
    
    This processor performs a simple text search and replace on the input
    records fields.  Call replace() ro regexp_replace() on processor to add
    replacement rules.
    
    This component uses the sames schema for output as is specified for the
    input.
    '''
    
    def __init__(self, schema, input_name='records', output_name='records'):
        '''Init
        
        @param schema: Schema to use for input and output records
        @param input_name: Name of the processor input for connections
        @param output_name: Name of the processor output for connections
        '''
        self.__schema = schema
        self.__input_name = input_name
        self.__output_name = output_name
        
        self.__replace_rules = list()     # (field, search, replace, case sens?)
        self.__re_replace_rules = list(  )# (field, pattern, replace, case sens?)
    
    
    def list_inputs(self):
        return [
            EtlProcessorDataPort(self.__input_name, self.__schema),
            ]
    
            
    def list_outputs(self):
        return [
            EtlProcessorDataPort(self.__output_name, self.__schema),
            ]
            
        
    def replace(self, field_name, search, replace, case_sensitive=True):
        self.__replace_rules.append( (field_name,
                                      search,
                                      replace,
                                      case_sensitive) )
    
    
    def regexp_replace(self, field_name, search_pat, replace, case_sensitive=True):
        self.__re_replace_rules.append( (field_name,
                                         re.compile(search_pat),
                                         replace,
                                         case_sensitive) )
