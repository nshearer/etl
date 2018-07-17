from netl import EtlComponent, EtlInput

class DebugRecords(EtlComponent):
    '''
    Processor that sends each record received to logging
    '''

    records = EtlInput()

    def run(self):
        log = self.logger
        for rec in self.records.all():
            log.debug("\n"+rec.format())
