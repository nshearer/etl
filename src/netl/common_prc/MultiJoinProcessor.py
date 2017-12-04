from abc import ABCMeta, abstractmethod

from JoinProcessor import JoinProcessor
from netl.recshelf.RecordShelf import RecordShelf

class MultiJoinProcessor(JoinProcessor):
    '''Join all the record on the right to one on the left
    
                       +----------------+                                     
                       |                +--->  joined                         
            left   +--->                |                                     
                       |  JoinProcssor  +--->  left_not_joined                
            right  +--->                |                                     
                       |                +--->  right_not_joined               
                       +----------------+                                     

    This is a variation on the JoinProcessor that holds all of the records on
    the right to be joined at once to a matching record on the left.  This means
    that all records will need to be received on both inputs before any records
    are released.
    
    The three key outputs are:
    
    joined: Contains each of the left records with all related right records
        joined in.
            
    left_not_joined: Contains each of the left records that was not matched to
        any record on the right input
                     
    right_not_joined: Contains each of the right records that was not matched to
        any record on the left input
                      
    To use, you must subclass and implement:
      - calc_left_join_key()
      - calc_right_join_key()
      - join_all_right_to_left() 
    '''
    __metacass__ = ABCMeta
    
    def __init__(self, name):
        super(MultiJoinProcessor, self).__init__(name)
        self.__shelf = RecordShelf()
        
        self.df_create_input_port('left')
        self.df_create_input_port('right')
        
        self.df_create_output_port('joined')
        self.df_create_output_port('left_not_joined')
        self.df_create_output_port('right_not_joined')
        
        
    @abstractmethod
    def join_left_to_right(self, right_record, left_record):
        '''Return a new record with the information joined'''
        
    