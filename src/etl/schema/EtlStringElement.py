from EtlSchemaElement import EtlSchemaElement

class EtlStringElement(EtlSchemaElement):
    '''String values'''

    def __init__(self, max_length=None, header=None):
        self.max_length=max_length
        super(EtlStringElement, self).__init__(header=header)


    def __eq__(self, other):
        if super(EtlStringElement, self).__eq__(other):
            if self.max_length is None and other.max_length is None:
                return True
            if self.max_length == other.max_length:
                return True
            return False
        return False