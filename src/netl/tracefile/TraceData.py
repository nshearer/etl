
from ..utils import RWLock

class TraceData:
    '''
    Collected trace data to be queried
    '''

    def __init__(self):
        self.lock = RWLock()