

from .. import EtlComponent, EtlInput, EtlOutput


class EtlFilter(EtlComponent):

    input = EtlInput()

    def __init__(self, filter_func, true_output, false_output):
        '''

        :param filter_func: Function to be passed record.  Should return True or False
        :param true_output: Name of output to send record if flter_func returns True
        :param false_output: Name of output to send record if flter_func returns False
        '''
        self.__true_output_name = true_output
        setattr(self, self.__true_output_name, EtlOutput())

        self.__false_output_name = false_output
        setattr(self, self.__false_output_name, EtlOutput())

        self.__filter_func = filter_func

        # Call last because this init adds outputs
        super(EtlFilter, self).__init__()



    def run(self):
        for rec in self.input.all():
            if self.__filter_func(rec):
                getattr(self, self.__true_output_name).output(rec)
            else:
                getattr(self, self.__false_output_name).output(rec)
