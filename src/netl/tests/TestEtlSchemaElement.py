import unittest

from test_data import PersonTestScehma

from etl.schema.EtlStringElement import EtlStringElement
from etl.schema.EtlBinaryElement import EtlBinaryElement
from etl.schema.EtlBoolElement import EtlBoolElement
from etl.schema.EtlIntElement import EtlIntElement
from etl.schema.EtlListElement import EtlListElement
from etl.schema.EtlRecordElement import EtlRecordElement


class TestEtlSchemaElement(unittest.TestCase):


    # Sorted alphabetically:
    def test_can_create_binary_element(self):
        EtlBinaryElement()
    def test_can_create_bool_element(self):
        EtlBoolElement()
    def test_can_create_list_element(self):
        EtlListElement(EtlStringElement())
    def test_can_create_reecord_element(self):
        EtlRecordElement(PersonTestScehma())
    def test_can_create_string_element(self):
        EtlStringElement()


    def test_elements_equal(self):
        self.assertEqual(EtlStringElement(), EtlStringElement())


    def test_string_with_same_len_equal(self):
        self.assertEqual(EtlStringElement(max_length=10),
                         EtlStringElement(max_length=10))


    def test_string_with_diff_len_not_equal(self):
        self.assertNotEqual(EtlStringElement(max_length=10),
                            EtlStringElement(max_length=20))
        
        self.assertNotEqual(EtlStringElement(max_length=None),
                            EtlStringElement(max_length=20))        


    def test_repr_matches(self):
        self.assertEqual(str(EtlStringElement()), str(EtlStringElement()))


    def test_header_changes_repr(self):
        elm_a = EtlStringElement()
        elm_b = EtlStringElement()

        elm_b.set_header("New Header")

        self.assertNotEqual(str(elm_a), str(elm_b))        


    def test_header_changes_repr_deterministic(self):
        elm_a = EtlStringElement()
        elm_b = EtlStringElement()

        elm_a.header = "New Header"
        elm_b.header = "New Header"

        self.assertEqual(str(elm_a), str(elm_b))


    def test_list_element(self):
        elm_a = EtlListElement(item_element=EtlStringElement())
        
        list_wrapper = elm_a.validate_and_set_value(["a", "b"])
        
        self.assertEqual(list_wrapper, ["a", "b"])
        