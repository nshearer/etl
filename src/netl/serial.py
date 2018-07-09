from threading import Lock

NEXT_ETL_SERIAL = 0
NEXT_ETL_LOCK = Lock()

class EtlSerial(object):
    '''Unique identification for EtlRecords'''

    def __init__(self):
        global NEXT_ETL_SERIAL, NEXT_ETL_LOCK
        with NEXT_ETL_LOCK:
            self.__value = NEXT_ETL_SERIAL
            NEXT_ETL_SERIAL += 1

    def __str__(self):
        return str(self.__value)

    def __eq__(self, serial):
        try:
            return self.__value == serial.__value
        except AttributeError:
            return False

    def __hash__(self):
        return hash(self.__value) # May change to string in the future
