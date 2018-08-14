from .. import EtlComponent, EtlInput

class DebugRecords(EtlComponent):
    '''
    Processor that sends each record received to logging
    '''

    records = EtlInput()

    def __init__(self, width=120):
        self.__width = width
        super(DebugRecords, self).__init__()

    def run(self):
        log = self.logger
        for rec in self.records.all():
            log.debug("\n"+rec.format(width=self.__width))
