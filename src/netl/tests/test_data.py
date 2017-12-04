'''Some test fixtures data fixtures'''

from netl.EtlRecord import EtlRecord
from netl.schema.EtlSchema import EtlSchema
from netl.schema.EtlStringElement import EtlStringElement
from netl.schema.EtlIntElement import EtlIntElement
from netl.schema.EtlBoolElement import EtlBoolElement

class PersonTestScehma(EtlSchema):
    # def __init__(self):
    #     super(PersonTestScehma, self).__init__()
    #     self.add_field('first', header="First Name")
    #     self.add_field('last', header="Last Name")
    #     self.add_field('age', header="Age", type_hint=self.INT)
    first = EtlStringElement().set_header("First Name")
    last = EtlStringElement().set_header("Last Name")
    age = EtlIntElement().set_header("Age")


class EmployeeTestSchema(PersonTestScehma):
    department = EtlStringElement().set_header("Department")
    age = None

        
class AnimalTestScehma(EtlSchema):
    # def __init__(self):
    #     super(AnimalTestScehma, self).__init__()
    #     self.add_field('common_name', header="Common Name")
    #     self.add_field('kingdom', header="Kingdom")
    #     self.add_field('family', header="Family")
    #     self.add_field('sane', header="Is Sane", type_hint=self.BOOL)
    common_name = EtlStringElement().set_header("Common Name")
    kingdom = EtlStringElement().set_header("Kingdom")
    family = EtlStringElement().set_header("Family")
    sane = EtlBoolElement().set_header("Is Sane")


        
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
    
    
    
    
