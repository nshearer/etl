from threading import Thread, Lock, Condition
from queue import Queue
from netl import TraceDB

class DatabaseService(Thread):
    '''
    Service to hold Database connection and answer queries

    Because sqlite3 connections can only be used in the thread they
    were created in, and we don't want to create new connections for
    every request, this thread holds a connection to the Database
    and responds to requests.
    '''

    def __init__(self, num, path):
        '''
        :param path: Path to the trace database
        '''

        # Used by DatabaseServicePool (covered under pool lock)
        self.num = num
        self.available = True

        # Used in thread
        super(DatabaseService, self).__init__(name="TraceDB-%d" % (self.num))
        self.__path = path
        self.db = None
        self.work_queue = Queue()
        self.results_queue = Queue()


    def run(self):

        # Create DB in the thrad
        self.db = TraceDB(self.__path, mode='r')

        # Serve requests
        while True:

            rtype, request = self.work_queue.get()

            if rtype == 'many':
                for result in request(self.db):
                    self.results_queue.put(('result', result))
                self.results_queue.put(('end', None))



class DatabaseServicePool:
    '''Set of connections to the TraceDB to answer requests on'''

    def __init__(self, path, threads=2):
        '''
        :param path: Path to the database to open up
        :param threads: Number of threads to create
        '''
        self.__services = list()
        self.__lock = Lock()
        self.__service_available = Condition()
        for i in range(threads):
            service = DatabaseService(i, path)
            service.start()
            self.__services.append(service)

             # Not sure if we need to pre-notify?
            with self.__service_available:
                self.__service_available.notify()


    def _service_is_available(self):
        '''Return True if a service is available (not a guarentee)'''
        with self.__lock:
            for service in self.__services:
                if service.available:
                    return True
        return False


    def _get_available_service(self):
        '''Get an available service to perform work'''
        while True:
            with self.__service_available:
                self.__service_available.wait_for(self._service_is_available)

            # Attempt to grab it
            with self.__lock:
                for service in self.__services:
                    if service.available:
                        service.available = False
                        return service


    def _release_service(self, service):
        '''Release a service that is no longer in use'''
        with self.__lock:
            with self.__service_available:
                service.available = True
                self.__service_available.notify()


    def request_multiple(self, request):
        '''
        Make a request of the TraceDB and yield back results

        :param request:
            Lambda function to make that will be passed a TraceDB
        :return: Generator or results
        '''

        service = self._get_available_service()
        service.work_queue.put(('many', request))
        try:
            while True:
                code, result = service.results_queue.get()
                if code == 'result':
                    yield result
                elif code == 'end':
                    break
        finally:
            self._release_service(service)


    def request_one(self, request):
        '''
        Make a request of the TraceDB and yield back results

        :param request:
            Lambda function to make that will be passed a TraceDB
        :return: Generator or results
        '''

        service = self._get_available_service()
        service.work_queue.put(('one', request))

        try:
            code, result = service.result_queue.get()
            if code == 'result':
                return result
            raise Exception("Expected result, got %s" % (code))

        finally:
            self._release_service(service)

