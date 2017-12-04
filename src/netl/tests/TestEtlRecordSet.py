import unittest

from test_data import test_person, PersonTestScehma
# Test Data:
#   (person,    "John",     "Doe",      22),
#   (person,    "Jane",     "Doe",      20),
#   (person,    "Mark",     "Smith",    41),
#   (animal,    "dog",      "Animalia", "Canidae",  True),
#   (animal,    "cat",      "Animalia", "Felidae",  False),

from etl.depricated import EtlRecordSet.EtlRecordSet


class TestEtlRecordSetConverts(unittest.TestCase):

    def testConvertsToDisk(self):
        rs = EtlRecordSet(size_until_disk=50)
        self.assertFalse(rs.on_disk)
        
        for i in range(5):
            person = test_person(0)
            person.freeze()
            rs.add_record(person)
            
        self.assertTrue(rs.on_disk)


class TestEtlRecordSet(unittest.TestCase):
    
    def _createRecordSet(self):
        return EtlRecordSet(size_until_disk=50)
    

    def testAddRecord(self):
        rs = self._createRecordSet()
        person = test_person(0)
        person.freeze()
        
        rs.add_record(person)
        
    
    def testGetRecord(self):
        rs = self._createRecordSet()
        
        person = test_person(0)
        person.freeze()
        rs.add_record(person)
        
        person1 = test_person(1)
        person1.freeze()
        rs.add_record(person1)
        
        self.assertEqual(rs.get_record(person.serial), person)

        
    def testCount(self):
        rs = self._createRecordSet()
        
        initial = rs.count
        
        person = test_person(0)
        person.freeze()
        rs.add_record(person)
        
        person1 = test_person(1)
        person1.freeze()
        rs.add_record(person1)
        
        self.assertEqual(rs.count - initial, 2)

        
    def testSize(self):
        rs = self._createRecordSet()
        
        initial = rs.size
        
        person = test_person(0)
        person.freeze()
        rs.add_record(person)
        
        person1 = test_person(1)
        person1.freeze()
        rs.add_record(person1)
        
        self.assertEqual(rs.size - initial, person.size + person1.size)

        
    def testRemoveRecord(self):
        rs = self._createRecordSet()
        
        person = test_person(0)
        person.freeze()
        rs.add_record(person, ['tagA', ])
        
        person1 = test_person(1)
        person1.freeze()
        rs.add_record(person1, ['tagA', ])
        
        self.assertTrue(rs.has_record(person.serial))
        self.assertEqual(sorted(list(rs.find_records_with_tag('tagA'))),
                         sorted([person, person1]))
        
        rs.remove_record(person.serial)

        self.assertFalse(rs.has_record(person.serial))
        self.assertEqual(sorted(list(rs.find_records_with_tag('tagA'))),
                         sorted([person1]))
        
        
    def testHasRecord(self):
        rs = self._createRecordSet()
        
        person = test_person(0)
        person.freeze()
        rs.add_record(person)
        
        person2 = test_person(1)
        person2.freeze()
        rs.add_record(person2)
        
        self.assertTrue(rs.has_record(person.serial))
        
        
    def testAddRecordWithTag(self):
        rs = self._createRecordSet()
        person = test_person(0)
        person.freeze()
        
        rs.add_record(person, ['tagA'])


    def testHasRecordWithTag(self):
        rs = self._createRecordSet()
        
        person = test_person(0)
        person.freeze()
        rs.add_record(person, ['tagA'])
        
        self.assertTrue(rs.has_record_with_tag('tagA'))
        
        
    def testFindOneRecordWithTag(self):
        rs = self._createRecordSet()
        
        person = test_person(0)
        person.freeze()
        rs.add_record(person, ['tagA'])
        
        self.assertEqual(list(rs.find_records_with_tag('tagA')),
                         [person, ])
        
        person1 = test_person(1)
        person1.freeze()
        rs.add_record(person1, ['tagB'])

        self.assertEqual(list(rs.find_records_with_tag('tagA')),
                         [person, ])
                
        
    def testFindMultipleRecordWithTag(self):
        rs = self._createRecordSet()
        
        person = test_person(0)
        person.freeze()
        rs.add_record(person, ['tagA'])
        
        person1 = test_person(1)
        person1.freeze()
        rs.add_record(person1, ['tagA'])
        
        self.assertEqual(list(sorted(rs.find_records_with_tag('tagA'))),
                         sorted([person, person1]))
        
        
        
class TestEtlRecordSetAfterConvert(TestEtlRecordSet):
    
    def _createRecordSet(self):
        rs = EtlRecordSet(size_until_disk=50)
        for i in range(5):
            person = test_person(0)
            person.freeze()
            rs.add_record(person)
        assert(rs.on_disk)
        return rs
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()