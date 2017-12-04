
from netl.EtlProcessor import EtlProcessor

class RecordSort(EtlProcessor):
    '''Sort records
    
    To calculate sort keys, use one of:
     
      - Override calc_record_sort_value()
      - Provide record_sort_key_func callback/lambda
      
                 +----------------+                             
                 |                |                             
        in +----->   RecordSort   +-----> sorted               
                 |                |                             
                 +----------------+                             
    '''
    
    def __init__(self, name, record_sort_key_func=None):
        super(RecordSort, self).__init__(name=name)
        self.__sort_func = record_sort_key_func
        self.df_create_input_port('in')
        self.df_create_output_port('sorted')
        
    
    def calc_record_sort_value(self, record):
        if self.__sort_func is None:
            raise Exception("Must provide record_sort_key_func")
            
        raise NotImplementedError("TODO")