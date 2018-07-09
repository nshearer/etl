

from .EtlResource import EtlResource


class EtlParameter(EtlResource):
    '''
    A (constant) parameter to put into the ETL session for components to read
    '''

    def __init__(self, name, value):
        super(EtlParameter, self).__init__(key=name, resource=value, thread_safe=True)

