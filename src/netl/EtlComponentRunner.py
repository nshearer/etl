

from threading import Thread


class EtlComponentRunner(Thread):
    '''
    Runs an EtlComponent in a seperate process
    '''

    def __init__(self, component):
        self.component = component
        super(EtlComponentRunner, self).__init__(name=component.name)
        self.start()

    def run(self):
        self.component._execute()


