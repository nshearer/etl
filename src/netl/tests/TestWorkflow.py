'''
Created on Apr 18, 2014

@author: nshearer
'''
import unittest

from netl.EtlProcessorBase import EtlProcessorBase

from .test_data import test_person, test_animal
# Test Data:
#   (person,    "John",     "Doe",      22),
#   (person,    "Jane",     "Doe",      20),
#   (person,    "Mark",     "Smith",    41),
#   (animal,    "dog",      "Animalia", "Canidae",  True),
#   (animal,    "cat",      "Animalia", "Felidae",  False),

from netl.EtlRecord import EtlRecordFrozen

class TestWorkflow(unittest.TestCase):
    '''Test a full workflow'''
    
    def instantiate_base(self):
        EtlProcessorBase()

    # def test_workflow(self):
    #     self.assertFalse('Not yet implemented')

        