import unittest

from etl.schema.EtlStringElement import EtlStringElement

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
                         set(['first', 'last', 'department']))

        
    def testListFields(self):
        fields = dict()
        for name, eclass in PersonTestScehma().etl_list_fields():
            fields[name] = eclass

        self.assertEqual(fields['first'].header, 'First Name')
                    
        self.assertEqual(fields['last'].header, 'Last Name')
                    
        self.assertEqual(fields['age'].header, 'Age')
                    

    def testFieldGetItem(self):
        schema = PersonTestScehma()
        self.assertEqual(schema['last'].header, 'Last Name')


    def testSchemaEqual(self):
        self.assertEqual(PersonTestScehma(), PersonTestScehma())
        self.assertTrue(PersonTestScehma() == PersonTestScehma())


    def testSchemaWithAddedFieldNotEqual(self):
        schema_a = PersonTestScehma()
        schema_b = PersonTestScehma()

        schema_a.etl_add_field('hair_color', PersonTestScehma())

        self.assertFalse(schema_a == schema_b)
        

    def testAddField(self):
        schema = PersonTestScehma()
        schema.etl_add_field('hair_color',
            EtlStringElement(header="Hair Color"))

        self.assertEqual(set(schema.etl_list_field_names()),
                         set(['first', 'last', 'age', 'hair_color']))

        self.assertEqual(schema['hair_color'].header, "Hair Color")



if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()