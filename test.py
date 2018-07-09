
class EtlSchemaElement(object):
    '''Represents the type of a field (probably not mutable)'''
    def __init__(self, header=None):
        self.__header = header


class StringElement(EtlSchemaElement):
    '''Holds a simple string'''
    def __init__(self, header=None, max_length=1024):
        super(StringElement, self).__init__(
            header=header)
        self.__max_length = max_length


class IntElement(EtlSchemaElement):
    '''Holds a simple string'''
    def __init__(self, header=None):
        super(IntElement, self).__init__(
            header=header)


class EtlSchema(object):
    '''Base'''



class PersonSchema(EtlSchema):
    '''A person'''
    first_name = StringElement(header='First Name')
    last_name = StringElement(header='Last Name')
    nick = StringElement(header='Nickame')
    age = IntElement(header="Age")


class EmployeeSchema(PersonSchema):
    '''An employee'''
    age = None
    department = StringElement()


if __name__ == '__main__':

    people = PersonSchema()
    print str(people.first_name)
    print str(people.age)

    print '-'*20

    employees = EmployeeSchema()
    print str(employees.first_name)
    print str(employees.age)
    print str(employees.department)



