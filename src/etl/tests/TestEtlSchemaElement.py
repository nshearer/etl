import unittest

from etl.schema.EtlStringElement import EtlStringElement
from etl.schema.EtlBinaryElement import EtlBinaryElement
from etl.schema.EtlBoolElement import EtlBoolElement
from etl.schema.EtlIntElement import EtlIntElement
from etl.schema.EtlListElement import EtlListElement
from etl.schema.EtlRecordElement import EtlRecordElement


class TestEtlSchemaElement(unittest.TestCase):

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