import os, sys
import logging
from decimal import Decimal
from textwrap import dedent
from datetime import datetime, date
from collections import OrderedDict
from pprint import pformat

from netl_analyze import NetAnalyzeHtmlServer

from netl import EtlWorkflow, EtlRecord
from netl import EtlComponent, EtlInput, EtlOutput
from netl import EtlRecordShelf
from netl import DebugRecords

TEST_RECORDS = OrderedDict()

TEST_RECORDS['str'] = {
    'a': 'value',
}

TEST_RECORDS['basic_types'] = {
    'str_var': 'value',
    'int_var': 2,
    'float_var': 1.2,
    'date_var': date(2018, 1, 1),
    'datetime_var': datetime(2018, 1, 2, 10, 0, 2),
    'decimal_var': Decimal('5.123'),
    'multiline_var': dedent("""\
        Line 1
        Line 2
        Line 3
        """).strip(),
}

TEST_RECORDS['list_type'] = {
    'list_var': [
        'abc',
        'cde',
        ],
}

TEST_RECORDS['dict_type'] = {
    'dict_var': {
        'a': 'abc',
        'b': 'cde',
        },
}

TEST_RECORDS['set_type'] = {
    'set_var': set([
        'abc',
        'cde',
    ]),
}

TEST_RECORDS['all_col_types'] = {
    'list_var': [
        'abc',
        'cde',
        ],
    'dict_var': {
        'a': 'abc',
        'b': 'cde',
        'c': 3,
        },
    'set_var': set([
        'abc',
        'cde',
    ]),
}

TEST_RECORDS['embedded_collections'] = {
    'list_var': [
        'abc',
        'cde',
        [
            'a',
            'c',
        ],
    ],
    'dict_var': {
        'a': 1,
        'b': 'abc',
        'c': {
            'd': 'abc',
        },
        'd': [
            1,
            'a',
            Decimal('1.23'),
        ],
        's': set(['x', 'y', 1]),
    },
}


class TestValueGenerator(EtlComponent):

    test_data = EtlOutput()

    def run(self):
        for test_name, test_attrs in TEST_RECORDS.items():
            rec = EtlRecord(record_type='test')
            rec['test_name'] = test_name
            for name, value in test_attrs.items():
                rec[name] = value
            self.test_data.output(rec)


class ShelfTest(EtlComponent):

    test_values_in = EtlInput()
    test_values_out = EtlOutput()

    def run(self):
        shelf = EtlRecordShelf()
        for i, rec in enumerate(self.test_values_in.all()):
            shelf.add(i, rec)
        for rec in shelf.all(sorted=True):
            self.test_values_out.output(rec)


class TestValueCompare(EtlComponent):

    test_data = EtlInput()

    def run(self):
        for rec in self.test_data.all():
            test_name = rec['test_name']
            received_value = rec[name]
            for name, value in TEST_RECORDS[test_name].items():
                if received_value != value:
                    self.logger.error("\n".join([
                        "Value for test value %s does not match." % (test_name),
                        "Original:",
                        pformat(value),
                        "Received:",
                        pformat(received_value),
                        ]))



if __name__ == '__main__':

    wf = EtlWorkflow()

    wf.test_data = TestValueGenerator()

    wf.shelf_test = ShelfTest()
    wf.test_data.test_data.connect(wf.shelf_test.test_values_in)

    wf.check = TestValueCompare()
    wf.shelf_test.test_values_out.connect(wf.check.test_data)

    trace_path = 'test_attr_handlers.trace'
    wf.trace_to(trace_path, overwrite=True, keep=True)

    wf.std_logging(logging.DEBUG)

    wf.start()

    debug = NetAnalyzeHtmlServer(trace_path, 8080)
    print("Server analyzer at http://127.0.0.1:8080")
    debug.serve_forever()

    wf.wait()
    wf.finish()
