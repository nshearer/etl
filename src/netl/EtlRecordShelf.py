

class EtlRecordShelf:
    '''
    Class that can store large number of records efficiently

    Records are stored with an assigned key.  Multiple records can have
    the same key, and when retrieved all records that have a given key
    will be returned in a random'ish order.
    '''

    def __init__(self):
        self.__mem_records = dict() # [key] = list(record, ...)
        self.__mem_count = 0


    def add(self, key, record):
        '''
        Add record to the shelf

        :param key: key to use to retrieve the record
        :param record: record to be saved
        '''
        if key not in self.__mem_records:
            self.__mem_records[key] = list()
        self.__mem_records[key].append(record)


    def has(self, key):
        '''
        Check to see if shelf has any records for a given the key

        :param key: key value that was passed to add()
        :return: boolean
        '''
        return key in self.__mem_records


    def get(self, key, keep=False):
        '''
        Retrieve all records stored under the key

        :param key: key value that was passed to add()
        :param keep: If ture, leave records in the shelf
        :return: records returned retrieved back from shelf aftger add()
        '''

        try:
            for record in self.__mem_records[key]:
                yield record
        except KeyError:
            pass



