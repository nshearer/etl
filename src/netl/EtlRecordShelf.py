
import shelve


class EtlRecordShelf:
    '''
    Container to store records for later use.

    Will shelve records for later access.  Used primarily when a component
    needs to save records before processing can continue.  Each record is
    stored using
    '''