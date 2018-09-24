import os

from netl import EtlComponent, EtlInput, EtlOutput


class EtlSorter(EtlComponent):
    '''Sort records'''

    input = EtlInput()
    sorted = EtlOutput()

    def __init__(self, sort_key, reverse=False):
        '''
        :param sort_key: Callable to build a string sort key for each record
        '''
        self.__sort_key = sort_key
        self.__reverse = reverse
        super(EtlSorter, self).__init__()

    def run(self):

        # TODO: Implement with a record shelf to sort many records

        for rec in sorted(self.input.all(), key = self.__sort_key, reverse=self.__reverse):
            self.sorted.output(rec)