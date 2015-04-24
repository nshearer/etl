from EtlSchemaElement import EtlSchemaElement

class EtlRecordElement(EtlSchemaElement):
    '''An element that stores a record'''

    def __init__(self, record_schema):
        '''Init 

        @param record_schema:
            The schema of the record that this 
        '''
        self.__schema = record_schema
        super(EtlRecordElement, self).__init__()


    @property
    def record_schema(self):
        return self.__schema


    def _get_attr_for_str(self):
        '''Get attributes to include in string representation'''
        attrs = super(EtlListElement, self)._get_attr_for_str()
        attrs['schema'] = str(self.__schema)
        return attrs


    def __eq__(self, other):
        if super(EtlListElement, self).__eq__(other):
            if self.__schema == other.__schema:
                return True
            return False
        return False        