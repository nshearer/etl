from EtlSchemaElement import EtlSchemaElement

class EtlStringElement(EtlSchemaElement):
    '''String values'''

    def __init__(self, max_length=None, header=None):
        self.max_length=max_length
        super(EtlStringElement, self).__init__(header=header)