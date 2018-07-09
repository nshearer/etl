from netl.EtlProcessor import EtlProcessor

class GroupByProcessor(EtlProcessor):
    '''This processor groups input records by a given key
    
    To calculate sort keys, use one of:
     
      - Override calc_record_group_key()
      - Provide record_group_key_func callback/lambda
      
                 +----------------+                             
                 |                |                             
        in +-----> GroupProcessor +-----> grouped               
                 |                |                             
                 +----------------+                             
    '''
    
    def __init__(self, name, record_group_key_func=None):
        super(GroupByProcessor, self).__init__(name=name)
        self.__group_func = record_group_key_func
        self.df_create_input_port('in')
        self.df_create_output_port('grouped')
        
    
    def calc_record_group_key(self, grouped):
        if self.__group_func is None:
            raise Exception("Must provide record_group_key_func")
            
        raise NotImplementedError("TODO")    