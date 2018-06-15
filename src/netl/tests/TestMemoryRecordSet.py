
import unittest

from .test_data import test_person, PersonTestScehma
# Test Data:
#   (person,    "John",     "Doe",      22),
#   (person,    "Jane",     "Doe",      20),
#   (person,    "Mark",     "Smith",    41),
#   (animal,    "dog",      "Animalia", "Canidae",  True),
#   (animal,    "cat",      "Animalia", "Felidae",  False),

from netl.MemoryRecordSet import MemoryRecordSet

class TestMemoryRecordSet(unittest.TestCase):


    def testAddRecord(self):
        rs = MemoryRecordSet()
        person = test_person(0)
        person.freeze()
        
        rs.add_record(person)


    def testGetRecord(self):
        rs = MemoryRecordSet()
        
        person = test_person(0)
        person.freeze()
        rs.add_record(person)
        
        person1 = test_person(1)
        person1.freeze()
        rs.add_record(person1)
        
        self.assertEqual(rs.get_record(person.serial), person)

        
    def testCount(self):
        rs = MemoryRecordSet()
        
        person = test_person(0)
        person.freeze()
        rs.add_record(person)
        
        person1 = test_person(1)
        person1.freeze()
        rs.add_record(person1)
        
        self.assertEqual(rs.count, 2)

        
    def testSize(self):
        rs = MemoryRecordSet()
        
        person = test_person(0)
        person.freeze()
        rs.add_record(person)
        
        person1 = test_person(1)
        person1.freeze()
        rs.add_record(person1)
        
        self.assertEqual(rs.size, person.size + person1.size)

        
    def testRemoveRecord(self):
        rs = MemoryRecordSet()
        
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
        rs = MemoryRecordSet()
        
        person = test_person(0)
        person.freeze()
        rs.add_record(person)
        
        person2 = test_person(1)
        person2.freeze()
        rs.add_record(person2)
        
        self.assertTrue(rs.has_record(person.serial))
        
        
    def testAddRecordWithTag(self):
        rs = MemoryRecordSet()
        person = test_person(0)
        person.freeze()
        
        rs.add_record(person, ['tagA'])


    def testHasRecordWithTag(self):
        rs = MemoryRecordSet()
        
        person = test_person(0)
        person.freeze()
        rs.add_record(person, ['tagA'])
        
        self.assertTrue(rs.has_record_with_tag('tagA'))
        
        
    def testFindOneRecordWithTag(self):
        rs = MemoryRecordSet()
        
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
        rs = MemoryRecordSet()
        
        person = test_person(0)
        person.freeze()
        rs.add_record(person, ['tagA'])
        
        person1 = test_person(1)
        person1.freeze()
        rs.add_record(person1, ['tagA'])
        
        self.assertEqual(list(sorted(rs.find_records_with_tag('tagA'))),
                         sorted([person, person1]))
        
        
        
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()