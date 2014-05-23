import unittest

from test_data import test_person, PersonTestScehma
# Test Data:
#   (person,    "John",     "Doe",      22),
#   (person,    "Jane",     "Doe",      20),
#   (person,    "Mark",     "Smith",    41),
#   (animal,    "dog",      "Animalia", "Canidae",  True),
#   (animal,    "cat",      "Animalia", "Felidae",  False),


class TestEtlSchema(unittest.TestCase):


    def testCeckRecordStruct(self):
        person = test_person(0)
        self.assertIsNone(PersonTestScehma().check_record_struct(person))

    
    def testFieldNames(self):
        self.assertEqual(PersonTestScehma().list_field_names(),
                         ['first', 'last', 'age'])
        
        
    def testListFields(self):
        fields = PersonTestScehma().list_fields()

        self.assertEqual(fields[0]['name'], 'first')
        self.assertEqual(fields[0]['header'], 'First Name')
                    
        self.assertEqual(fields[1]['name'], 'last')
        self.assertEqual(fields[1]['header'], 'Last Name')
                    
        self.assertEqual(fields[2]['name'], 'age')
        self.assertEqual(fields[2]['header'], 'Age')
                    

    def testSchemaEqual(self):
        self.assertEqual(PersonTestScehma(), PersonTestScehma())
        
        
    def testClone(self):
        schema = PersonTestScehma()
        self.assertEqual(schema, schema.clone())

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()