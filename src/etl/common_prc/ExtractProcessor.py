
from etl.EtlProcessor import EtlProcessor

class ExtractProcessor(EtlProcessor):
    '''This processor is used to strip out all fields except a few
    
                 +------------------+                             
                 |                  |                             
        in +-----> ExtractProcessor +-----> out               
                 |                  |                             
                 +------------------+                             
    '''
    
    def __init__(self, name, out_schema, out_fields):
        super(ExtractProcessor, self).__init__(name=name)
        self.__schema = out_schema
        self.__fields = out_fields
        
        self.df_create_input_port('in')
        self.df_create_output_port('out')
        
    
