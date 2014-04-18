'''
Created on Apr 15, 2014

@author: nshearer
'''
from EtlBuildError import EtlBuildError

class InvalidProcessorName(EtlBuildError):
    '''A non-existent processor dataport name was used'''
