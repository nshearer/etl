'''Some test fixtures data fixtures'''

from etl.EtlRecord import EtlRecord
from etl.schema.EtlSchema import EtlSchema
from etl.schema.EtlStringElement import EtlStringElement
from etl.schema.EtlIntElement import EtlIntElement
from etl.schema.EtlBoolElement import EtlBoolElement

class PersonTestScehma(EtlSchema):
    # def __init__(self):
    #     super(PersonTestScehma, self).__init__()
    #     self.add_field('first', header="First Name")
    #     self.add_field('last', header="Last Name")
    #     self.add_field('age', header="Age", type_hint=self.INT)
    first = EtlStringElement(header="First Name")
    last = EtlStringElement(header="Last Name")
    age = EtlIntElement(header="Age")


class EmployeeTestSchema(PersonTestScehma):
    department = EtlStringElement(header="Department")
    age = None

        
class AnimalTestScehma(EtlSchema):
    # def __init__(self):
    #     super(AnimalTestScehma, self).__init__()
    #     self.add_field('common_name', header="Common Name")
    #     self.add_field('kingdom', header="Kingdom")
    #     self.add_field('family', header="Family")
    #     self.add_field('sane', header="Is Sane", type_hint=self.BOOL)
    common_name = EtlStringElement(header="Common Name")
    kingdom = EtlStringElement(header="Kingdom")
    family = EtlStringElement(header="Family")
    sane = EtlBoolElement(header="Is Sane")


        
def test_data_source():
    person = PersonTestScehma()
    animal = AnimalTestScehma()
    employee = EmployeeTestSchema()
    
    return [
        (person,    "John",     "Doe",      22),                    # Person 1
        (person,    "Jane",     "Doe",      20),                    # Person 2
        (person,    "Mark",     "Smith",    41),                    # Person 3
        (employee,  "John",     "Doe",      "Finance"),             # Employee 1
        (employee,  "Jane",     "Doe",      "Budget"),              # Employee 2
        (employee,  "Luke",     "Smith",    "Budget"),              # Employee 3
        (animal,    "dog",      "Animalia", "Canidae",  True),      # Animal 1
        (animal,    "cat",      "Animalia", "Felidae",  False),     # Animal 2
        ]

def test_person(i):
    test_data = test_data_source()
    assert(i < 3)
    return EtlRecord(test_data[i][0],
                     {'first': test_data[i][1],
                      'last': test_data[i][2],
                      'age': test_data[i][3]})
    
    
def test_employee(i):
    test_data = test_data_source()
    i = i + 3
    return EtlRecord(test_data[i][0],
                     {'first': test_data[i][1],
                      'last': test_data[i][2],
                      'department': test_data[i][3]})
    
    
def test_animal(i):
    test_data = test_data_source()
    i = i + 6
    assert(i<len(test_data))
    return EtlRecord(test_data[i][0],
                     {'common_name': test_data[i][1],
                      'kingdom': test_data[i][2],
                      'family': test_data[i][3],
                      'sane': test_data[i][4]})
    
    
    
    
