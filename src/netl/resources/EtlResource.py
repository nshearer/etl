
from random import shuffle
from threading import Lock, Condition

class EtlResourceHandle:
    '''Wrapper that the resource is placed in when given to a component'''

    def __init__(self, resource, pool):
        self.__resource_wrapper = resource
        self.__pool = pool


    def __del__(self):
        if self.__resource_wrapper is not None:
            print("ERROR: resource %s wasn't released!" % (self.__resource_wrapper.key))
            self.release()


    def release(self):
        '''Release the resource so that it is no longer in use'''

        # Use EtlResourcePool condition Nto otify anyone waiting for this resource that it is available
        with self.__pool.resource_avail_cond:
            self.__resource_wrapper._released()
            self.__pool.resource_avail_cond.notify()
        self.__resource_wrapper = None


    @property
    def resource(self):
        if self.__resource_wrapper is None:
            raise Exception("Resource has been released")
        return self.__resource_wrapper.resource

    @property
    def v(self):
        return self.__resource_wrapper.resource


class EtlResource:
    '''
    Wraps a resource that is needed by the ETL

    Examples include:

     - Configuration parameters
     - Database connections

    '''

    def __init__(self, key, resource, thread_safe=False):
        '''
        :param key: Key to access the resource with.  Multiple resources can have same name.
            Resources are accessed in components by specifying their key value.
            Multiple resources can have the same name.  In that case, they form a pool and
            the requesting component will get one or the resources randomly that is not in use.
        :param resource: The object or value that is the actual resource (DB connection, variable, etc)
        :param thread_safe: Can this resource be given to multiple components at once
        '''
        self.__key = key
        self.__resource = resource
        self.__safe = thread_safe

        self.__lock = Lock()
        self.__locked = False # True if not thread_safe and in use


    @property
    def resource(self):
        return self.__resource


    def peak_available(self):
        '''
        Check to see if this resource is available

        Doesn't guarentee will be available when grab_handle is called
        '''
        return not self.__locked


    def grab_handle(self):
        '''Attempt to get this resource'''
        with self.__lock:
            if not self.__locked:
                # Successfully grabed
                if not self.__safe:
                    self.__locked = True
                return EtlResourceHandle(self)

        # Failed to get handle
        return None


    def _released(self):
        '''Inform the resource that it can be used by someone else'''
        with self.__lock:
            if self.__locked:
                self.__locked = False


class EtlResourcePool:
    '''
    A collection of resources with the same key
    '''

    def __init__(self):
        #self.pool_lock = Lock()
        self.resource_avail_cond = Condition()
        self.resources = list()


class EtlResourceCollection:
    '''The collection of resources for an ETL session'''

    def __init__(self):
        self.__lock = Lock()
        self.__pools = dict()   # [key] = Pool


    def add(self, resource):
        with self.__lock:
            if resource.key not in self.__pools:
                self.__pools[resource.key] = EtlResourcePool()
            self.__pools[resource.key].resources.append(resource)


    def get(self, key):
        '''Get an available resource by it's key (blocking)'''

        # Get the pool
        with self.__lock:
            try:
                pool = self.__resources[key]
            except KeyError:
                raise KeyError("Invalid resource key requested: " + str(key))

        # Get an item from the pool
        handle = None
        with pool.resource_avail_cond:
            while handle is None:
                # Pick a random availble resource
                avail = [r for r in pool.resources if r.peak_available()]
                if len(avail) > 1:
                    for resource in list(shuffle(avail)):
                        handle = resource.grab_handle()
                        if handle is not None:
                            break

                # If couldn't get a handle, wait for one to become available
                if handle is None:
                    while len([r for r in pool.resources if r.peak_available()]) == 0:
                        self.resource_avail_cond.wait()

                else:
                    return handle


