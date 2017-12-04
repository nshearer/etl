
class TempoaryDirectory(object):
    '''Creates a temporary directory for storing ETL data
    
    Directory is deleted (with all contents) when class is cleaned up
    '''
    
    def __init__(self, parent=None, prefix=None):
        '''Init
        
        @param parent: Path to parent directory to place this directory in
        @param prefix: Prefix to put on directory name
        '''
        self.__path = 
         
         
    @property
    def path(self):
        return self.__path
    
    
    def clean(self):
        # TODO: Delete directory contents
        
        
    def __del__(self):
        self.clean()