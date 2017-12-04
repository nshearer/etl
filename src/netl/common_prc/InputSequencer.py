
from netl.EtlProcessor import EtlProcessor

class InputSequencer(EtlProcessor):
    '''Passes through one complete input before another
    
    This is used to ensure that one stream of records completes before another
    stream of records is allowed to pass through.
    
                        +----------------+            
             first +---->                +----> first 
                        | InputSequencer |            
            second +---->                +----> second
                        +----------------+            

    All input from first will be passed through until first is finished, then
    input second will be passed through.  A lock is placed on second, so input
    will be blocked (not stashed) until first is done.
    '''
    
    def __init__(self, name):
        super(InputSequencer, self).__init__(name=name)

        self.df_create_input_port('first')
        self.df_create_input_port('second')
        self._lock_input_port('second')
        
        self.df_create_output_port('first')
        self.df_create_output_port('second')
    