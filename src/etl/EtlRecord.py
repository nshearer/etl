'''
Created on Dec 27, 2012

@author: nshearer
'''
from UserDict import DictMixin

class EtlRecord(DictMixin):
    '''Container for values for a single record'''
    
    def __init__(self, values = dict(), index=None):
        self.__values = values
        self.__index = index
        
        
    def field_names(self):
        return self.values.keys()
    
    
    def create_msg(self, msg):
        '''Generate a message about this record'''
        if self.__index is not None:
            return "%s: %s: Record[[%s]]" % (msg, self.__index, str(self.values))
        else:
            return "%s: Record[[%s]]" % (msg, str(self.values))
        
    @property
    def values(self):
        return self.__values.copy()
    
    
    def value(self, name):
        return self.__values[name]
    
    
    @property
    def index(self):
        return self.__index
    
    
    def __getitem__(self, name):
        return self.value(name)

