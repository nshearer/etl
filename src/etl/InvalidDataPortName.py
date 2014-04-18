from EtlBuildError import EtlBuildError

class InvalidDataPortName(EtlBuildError):
    '''A non-existant dataport was referenced for a processor'''