
from etl.EtlProcessor import EtlProcessor

class UniqueFilter(EtlProcessor):
    '''Pass through unique records
    
    Unique records are determined serialized values unless one of these other
    methods are employed:
     
      - calc_record_repr() is overridden
      - record_repr_func callback/lambda is provided
      
                 +----------------+                             
                 |                |                             
        in +----->  UniqueFilter  +-----> uniques               
                 |                |                             
                 +----------------+                             
    '''
    
    def __init__(self, name, record_repr_func=None):
        super(UniqueFilter, self).__init__(name=name)
        self.__record_repr_func = record_repr_func
        self.df_create_input_port('in')
        self.df_create_output_port('uniques')
        
    
    def calc_record_repr(self, record):
        if self.__record_repr_func is not None:
            key = self.__record_repr_func(record)
        else:
            key = record.build_full_record_repr()
            
        raise NotImplementedError("TODO")