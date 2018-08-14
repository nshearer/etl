'''Common exceptions for netl library'''



class NoMoreData(Exception): pass


class InvalidEtlRecordKey(KeyError): pass

class EtlRecordFrozen(Exception):
    def __init__(self):
        msg = "Attempting to modify a frozen EtlRecord"
        super(EtlRecordFrozen, self).__init__(msg)        


class SessionNotCreatedYet(Exception): pass


class NoAttributeValueHandler(Exception): pass
