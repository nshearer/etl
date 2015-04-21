from EtlSchemaElement import EtlSchemaElement

class EtlAnyElement(EtlSchemaElement):
    '''Don't want to specify the type of the field value?  Use this.'''

    def __init__(self):
        pass