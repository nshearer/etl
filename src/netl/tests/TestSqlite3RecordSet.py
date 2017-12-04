import os
import unittest

from test_data import test_person, PersonTestScehma
# Test Data:
#   (person,    "John",     "Doe",      22),
#   (person,    "Jane",     "Doe",      20),
#   (person,    "Mark",     "Smith",    41),
#   (animal,    "dog",      "Animalia", "Canidae",  True),
#   (animal,    "cat",      "Animalia", "Felidae",  False),

from netl.Sqlite3RecordSet import Sqlite3RecordSet

class TestSqlite3RecordSet(unittest.TestCase):


    def testAddRecord(self):
        rs = Sqlite3RecordSet()
        person = test_person(0)
        person.freeze()
        
        rs.add_record(person)


    def testGetRecord(self):
        rs = Sqlite3RecordSet()
        
        person = test_person(0)
        person.freeze()
        rs.add_record(person)
        
        person1 = test_person(1)
        person1.freeze()
        rs.add_record(person1)
        
        self.assertEqual(rs.get_record(person.serial), person)

        
    def testCount(self):
        rs = Sqlite3RecordSet()
        
        person = test_person(0)
        person.freeze()
        rs.add_record(person)
        
        person1 = test_person(1)
        person1.freeze()
        rs.add_record(person1)
        
        self.assertEqual(rs.count, 2)

        
    def testSize(self):
        rs = Sqlite3RecordSet()
        
        person = test_person(0)
        person.freeze()
        rs.add_record(person)
        
        person1 = test_person(1)
        person1.freeze()
        rs.add_record(person1)
        
        self.assertEqual(rs.size, person.size + person1.size)

        
    def testRemoveRecord(self):
        rs = Sqlite3RecordSet()
        
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
        rs = Sqlite3RecordSet()
        
        person = test_person(0)
        person.freeze()
        rs.add_record(person)
        
        person2 = test_person(1)
        person2.freeze()
        rs.add_record(person2)
        
        self.assertTrue(rs.has_record(person.serial))
        
        
    def testAddRecordWithTag(self):
        rs = Sqlite3RecordSet()
        person = test_person(0)
        person.freeze()
        
        rs.add_record(person, ['tagA'])


    def testHasRecordWithTag(self):
        rs = Sqlite3RecordSet()
        
        person = test_person(0)
        person.freeze()
        rs.add_record(person, ['tagA'])
        
        self.assertTrue(rs.has_record_with_tag('tagA'))
        
        
    def testFindOneRecordWithTag(self):
        rs = Sqlite3RecordSet()
        
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
        rs = Sqlite3RecordSet()
        
        person = test_person(0)
        person.freeze()
        rs.add_record(person, ['tagA'])
        
        person1 = test_person(1)
        person1.freeze()
        rs.add_record(person1, ['tagA'])
        
        self.assertEqual(list(sorted(rs.find_records_with_tag('tagA'))),
                         sorted([person, person1]))
        
        
    def testDbRemoved(self):
        rs = Sqlite3RecordSet()
        path = rs.db_path
        self.assertTrue(os.path.exists(path))
        
        rs = None
        self.assertFalse(os.path.exists(path))
        
        
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()