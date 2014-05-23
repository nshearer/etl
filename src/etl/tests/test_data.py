'''Some test fixtures data fixtures'''

from etl.EtlRecord import EtlRecord
from etl.EtlSchema import EtlSchema

class PersonTestScehma(EtlSchema):
    def __init__(self):
        super(PersonTestScehma, self).__init__()
        self.add_field('first', header="First Name")
        self.add_field('last', header="Last Name")
        self.add_field('age', header="Age", type_hint=self.INT)
        
        
class AnimalTestScehma(EtlSchema):
    def __init__(self):
        super(AnimalTestScehma, self).__init__()
        self.add_field('common_name', header="Common Name")
        self.add_field('kingdom', header="Kingdom")
        self.add_field('family', header="Family")
        self.add_field('sane', header="Is Sane", type_hint=self.BOOL)
        
        
def test_data_source():
    person = PersonTestScehma()
    animal = AnimalTestScehma()
    
    return [
        (person,    "John",     "Doe",      22),
        (person,    "Jane",     "Doe",      20),
        (person,    "Mark",     "Smith",    41),
        (animal,    "dog",      "Animalia", "Canidae",  True),
        (animal,    "cat",      "Animalia", "Felidae",  False),
        ]

def test_person(i):
    test_data = test_data_source()
    assert(i < 3)
    return EtlRecord(test_data[i][0],
                     {'first': test_data[i][1],
                      'last': test_data[i][2],
                      'age': test_data[i][3]})
    
    
def test_animal(i):
    test_data = test_data_source()
    i = i + 3
    assert(i<len(test_data))
    return EtlRecord(test_data[i][0],
                     {'common_name': test_data[i][1],
                      'kingdom': test_data[i][2],
                      'family': test_data[i][3],
                      'sane': test_data[i][4]})
    
    
    
    
