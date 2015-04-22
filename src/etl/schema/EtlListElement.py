from EtlSchemaElement import EtlSchemaElement

class EtlListElement(EtlSchemaElement):
    '''An element that stores a list of items'''

    def __init__(self, item_element, header=None):
        '''Init 

        @param item_element:
            An EtlSchemaElement for the items of the list
        '''
        self.__item_element = item_element
        super(EtlListElement, self).__init__(header=header)


    @property
    def item_element(self):
        return self.__item_element


    def __eq__(self, other):
        if super(EtlListElement, self).__eq__(other):
            if self.item_element == other.item_element:
                return True
            return False
        return False