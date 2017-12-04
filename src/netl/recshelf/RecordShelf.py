from netl.TemporaryDir import TempoaryDirectory

class RecordShelf(object):
    '''Class for storing records temporarily'''
    
    def __init__(self, temp_dir, shelf_name):
        self.__dir = TempoaryDirectory(temp_dir, shelf_name + ".shelf")