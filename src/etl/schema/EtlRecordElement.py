from EtlSchemaElement import EtlSchemaElement

class EtlRecordElement(EtlSchemaElement):
    '''An element that stores a record'''

    def __init__(self, record_schema, header=None):
        '''Init 

        @param record_schema:
            The schema of the record that this 
        '''
        self.__schema = record_schema
        super(EtlRecordElement, self).__init__(header=header)


    @property
    def record_schema(self):
        return self.__schema