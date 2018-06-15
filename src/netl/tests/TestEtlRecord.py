'''
Created on Apr 18, 2014

@author: nshearer
'''
import unittest

from .test_data import test_person, test_animal
# Test Data:
#   (person,    "John",     "Doe",      22),
#   (person,    "Jane",     "Doe",      20),
#   (person,    "Mark",     "Smith",    41),
#   (animal,    "dog",      "Animalia", "Canidae",  True),
#   (animal,    "cat",      "Animalia", "Felidae",  False),

from netl.EtlRecord import EtlRecordFrozen

class TestEtlRecord(unittest.TestCase):
    

    def testSerial(self):
        self.assertIsNotNone(test_person(0).serial,
                             "Serial should not be None")
        
        
    def testEquals(self):
        self.assertEqual(test_person(0), test_person(0))
        
        
    def testIfValuesNotEquals(self):
        self.assertNotEqual(test_person(0), test_person(1))
        
        
    def testIfSchemasNotEquals(self):
        self.assertNotEqual(test_person(0), test_animal(0))
        
        
    def testFieldNames(self):
        self.assertEqual(set(test_person(0).field_names()),
                          set(['first', 'last', 'age']))
        
    
#     def testFieldNamesWithoutSchema(self):
#         rec = test_person(0)
#         rec.set_schema(None)
#         self.assertEquals(sorted(rec.field_names()),
#                           sorted(['first', 'last', 'age']))
        
    
    def testGetFieldValue(self):
        rec = test_person(0)
        self.assertEqual(rec['first'], "John")
        self.assertEqual(rec['last'], "Doe")
        self.assertEqual(rec['age'], 22)
        
        
    def testSetFieldValue(self):
        rec = test_person(1)
        rec['first'] = "Jane"
        rec['age'] = 20
        self.assertEqual(rec, test_person(1))
        
        
    def testFreeze(self):
        rec = test_person(1)
        rec.freeze()
        self.assertTrue(rec.is_frozen,
                        "Record should be frozen")
        
        
    def testCantUpdateFrozen(self):
        rec = test_person(1)
        rec.freeze()
        with self.assertRaises(EtlRecordFrozen):
            rec['first'] = "new"
            
            
    def testAssertNotFrozen(self):
        rec = test_person(0)
        rec.assert_not_frozen()
        rec.freeze()
        with self.assertRaises(EtlRecordFrozen):
            rec.assert_not_frozen()
        
    
    def testClone(self):
        rec = test_person(0)
        cloned = rec.clone()
        self.assertEqual(rec, cloned) 
        
        
    def testSize(self):
        rec = test_person(0)
        self.assertLess(abs(rec.size - 20), 5) # w/in 5 of 20
        
        
    def testRecordSource(self):
        dst = test_person(1)
        src0 = test_person(0)
        src1 = test_animal(0)
        
        dst.note_src_record(src0)
        dst.note_src_record(src1)
        
        self.assertIn(src0.serial, dst.get_src_record_serials())
        self.assertIn(src1.serial, dst.get_src_record_serials())
        

    def testSourceProcessor(self):
        rec = test_person(0)
        rec.set_source('processor1', 'output1')
        self.assertEqual(rec.source_processor_name, 'processor1')
        self.assertEqual(rec.source_processor_output_name, 'output1')
        
        
    def testValues(self):
        self.assertEqual(test_person(0).values,
                         {'first': "John", 'last': "Doe", 'age': 22})
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
    