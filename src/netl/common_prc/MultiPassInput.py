

from .. EtlInput import EtlInput
from ..exceptions import NoMoreData

class MultiPassInput(EtlInput):
    '''
    Input which can be iterated over multiple times.

    All records received will be stored for multiple passes.

    It is up to the component with the input to eventually call
    done() to release the stored records.
    '''

    def __init__(self, maxsize=EtlInput.DEFAULT_MAXSIZE, class_port=True):
        '''
        :param maxsize: Maximum number of records to queue until input queue will block
        :param class_port: Is this a port defined on the class as opposed to a component instance
        '''
        super(MultiPassInput, self).__init__(maxsize=maxsize, class_port=class_port)

        self.__maxsize = maxsize

        if not class_port:
            self.__cleaned = False
            self.__first_pass_finished = False
            self.__records = list()


    def __del__(self):
        if not self.__cleaned:
            print("ERROR: %s.done() was never called" % (str(self)))


    def _assert_not_cleaned(self):
        if self.__cleaned:
            raise Exception("done() already called")


    def all(self, envelope=False):
        '''
        Return all records

        For multi-pass, will return all records received on the port each time called
        '''
        self._assert_not_cleaned()

        # Finish retrieving all input records
        if not self.__first_pass_finished:
            for rec in super(MultiPassInput, self).all(envelope=envelope):
                yield rec

        # Else, return all items again
        else:
            for envl in self.__records:
                if envelope:
                    yield envl
                else:
                    yield envl.record


    def get(self, envelope=False):
        '''Return a single record'''
        self.assert_is_instance_port()
        self._assert_not_cleaned()

        try:
            # Still getting first pass or records?
            if not self.__first_pass_finished:

                # Get next record from input
                envl = super(MultiPassInput, self).get(envelope=True)

                # Store a copy of the record
                # TODO: Need to shelve to disk if we get lots of records
                self.__records.append(envl)

                # Pass to component
                if envelope:
                    return envl
                else:
                    return envl.record

            else:
                # Else, throw NoMoreData().   Use all() to iterate again
                raise NoMoreData("No more data being received.  Use all() to iterate again")

        except NoMoreData as e:
            self.__first_pass_finished = True
            raise e


    def create_instance_port(self):
        return MultiPassInput(maxsize=self.__maxsize, class_port=False)


    def done(self):
        '''Call to release stored record cache'''
        self.__records = None
        self.__cleaned = True