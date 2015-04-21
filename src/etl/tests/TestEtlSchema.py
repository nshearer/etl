import unittest



from test_data import test_person, PersonTestScehma, EmployeeTestSchema
# Test Data:
#   (person,    "John",     "Doe",      22),
#   (person,    "Jane",     "Doe",      20),
#   (person,    "Mark",     "Smith",    41),
#   (animal,    "dog",      "Animalia", "Canidae",  True),
#   (animal,    "cat",      "Animalia", "Felidae",  False),


class TestEtlSchema(unittest.TestCase):


    def testCeckRecordStruct(self):
        person = test_person(0)
        self.assertIsNone(PersonTestScehma().etl_check_record_struct(person))

    
    def testFieldNames(self):
        self.assertEqual(set(PersonTestScehma().etl_list_field_names()),
                         set(['first', 'last', 'age']))
        
    def testInheritedFieldNames(self):
        self.assertEqual(set(EmployeeTestSchema().etl_list_field_names()),
                         set(['first', 'last', 'age', 'department']))

        
    def testListFields(self):
        fields = dict()
        for name, eclass in PersonTestScehma().etl_list_fields():
            fields[name] = eclass

        self.assertEqual(fields['first'].header, 'First Name')
                    
        self.assertEqual(fields['last'].header, 'Last Name')
                    
        self.assertEqual(fields['age'].header, 'Age')
                    

    def testSchemaEqual(self):
        self.assertEqual(PersonTestScehma(), PersonTestScehma())
        
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()