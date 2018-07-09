from threading import Lock

from .html_content import NETL_ANALYZE_HTML

class WebSource:
    '''All of the source files needed to provide the NETL Analyze web app'''

    def __init__(self):
        self.__lock = Lock()

        # Index the files availabe
        self.__files = set(NETL_ANALYZE_HTML.namelist())


    def has(self, name):
        '''Check to see if we have a file'''
        with self.__lock: # Lock needed for in set?
            return name in self.__files


    def get(self, name):
        '''Get a source file'''
        with self.__lock:
            try:
                return NETL_ANALYZE_HTML.open(name).read()
            except KeyError:
                raise KeyError("No file named '%s' in source archive" % (name))

