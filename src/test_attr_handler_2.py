import os, sys
import logging
from decimal import Decimal
from textwrap import dedent
from datetime import datetime, date, timedelta
from collections import OrderedDict
from pprint import pformat
from frozendict import frozendict

from netl_analyze import NetAnalyzeHtmlServer

from netl import EtlWorkflow, EtlRecord
from netl import EtlComponent, EtlInput, EtlOutput
from netl import EtlRecordShelf
from netl import DebugRecords

TEST_RECORDS = OrderedDict()

TEST_RECORDS['movie'] = {
    'actors': [
        {
            'name': 'Nathan Fillion',
            'birthdate': date(1971, 3, 27),
            'addedd': datetime.now(),
            'also in': [
                {
                    'title': "The Rookie",
                    'year': 2018,
                    'role': 'pre-producer',
                },
                {
                    'title': "Henchmen",
                    'year': 2018,
                    'role': 'voice',
                },
            ],
        },
        {
            'name': 'Dina Torres',
            'birthdate': date(1969, 4, 25),
            'addedd': datetime.now(),
            'also in': [
                {
                    'title': "Second City",
                    'year': 2019,
                    'role': 'actor',
                },
                {
                    'title': "Angie  Tribeca",
                    'year': 2018,
                    'role': 'actor',
                },
            ],
        },
    ],
    'storyline': dedent("""\
        In the future, a spaceship called Serenity is harboring a passenger with a deadly secret
        Six rebels on the run. An assassin in pursuit. When the renegade crew of Serenity agrees
        to hide a fugitive on their ship, they find themselves in an awesome action-packed
        battle between the relentless military might of a totalitarian regime who will destroy
        anything - or anyone - to get the girl back and the bloodthirsty creatures who roam the
        uncharted areas of space. But, the greatest danger of all may be on their ship."""),
    'keywords': [
        {'term': 'on the run',          'order': 1},
        {'term': 'smuggler',            'order': 2},
        {'term': 'spacecraft',          'order': 3},
        {'term': 'megacorporation',     'order': 4},
        {'term': 'reavers',             'order': 5},
    ],
    'rating': 7.9,
    'sound': ('DTS', 'Dolby Digital', 'SDDS'),
    'oppening week': Decimal('10086680'),
    'box-office': Decimal('25514517.14'),
    'runtime': timedelta(hours=1, minutes=59),
    'faults': None,
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


class FrozenValueDebug(EtlComponent):

    test_values_in = EtlInput()
    test_data = EtlOutput()

    def run(self):

        def i(num, line):
            lines = line.split("\n")
            for i, line in enumerate(lines):
                lines[i] = ' '*(num*4) + line
            return "\n".join(lines)

        def format_dict(d, indent, dkey, dval):
            if dval.__class__ is dict or dval.__class__ is frozendict:
                d.append(i(indent, "'%s': {" % (dkey)))
                for subkey, subval in dval.items():
                    format_dict(d, indent+1, subkey, subval)
                d.append(i(indent, "}"))
            else:
                d.append(i(indent, "'%s': %s" % (dkey, repr(dkey))))

        for rec in self.test_values_in.all():

            d = list()

            d.append("="*80)

            d.append("Serial:  " + str(rec._EtlRecord__serial))
            d.append("Attrs:")
            for name, attr in rec._EtlRecord__values.items():
                d.append(i(1, "'%s': %s(" % (name, attr.__class__.__name__)))
                d.append(i(2, "value          = " + repr(attr.value)))
                d.append(i(2, "orig_class     = " + repr(attr.orig_value_clsname)))
                d.append(i(2, "freeze_success = " + repr(attr.freeze_successful)))
                d.append(i(2, "is_frozen_col  = " + repr(attr.is_frozen_collection)))
                format_dict(d, 2, 'meta', attr.freeze_collection_metadata())

            d.append("="*80)
            self.logger.debug("\n".join(d))

        self.test_values_out.output(rec)


class ShelfTest(EtlComponent):


    test_values_in = EtlInput()
    test_data = EtlOutput()

    def run(self):
        shelf = EtlRecordShelf(self.session, limit=0)
        for i, rec in enumerate(self.test_values_in.all()):
            shelf.add(i, rec)
        for rec in shelf.all():
            self.test_values_out.output(rec)


class TestValueCompare(EtlComponent):

    test_values_in = EtlInput()

    def run(self):
        for rec in self.test_values_in.all():
            rec = rec.copy()
            test_name = rec['test_name']
            for name, value in TEST_RECORDS[test_name].items():
                if rec[name] != value:
                    self.logger.error("\n".join([
                        "Value for test value %s does not match." % (test_name),
                        "Original:",
                        pformat(value),
                        "Received:",
                        pformat(rec[name]),
                        ]))



if __name__ == '__main__':

    wf = EtlWorkflow()

    wf.test_data = TestValueGenerator()

    wf.frozen_debug = FrozenValueDebug()
    wf.test_data.test_data.connect(wf.frozen_debug.test_values_in)

    wf.shelf_test = ShelfTest()
    wf.frozen_debug.test_data.connect(wf.shelf_test.test_values_in)

    wf.check = TestValueCompare()
    wf.shelf_test.test_data.connect(wf.check.test_values_in)

    trace_path = 'test_attr_handlers.trace'
    wf.trace_to(trace_path, overwrite=True, keep=True)

    wf.std_logging(logging.DEBUG)

    wf.start()

    #debug = NetAnalyzeHtmlServer(trace_path, 8080)
    #print("Server analyzer at http://127.0.0.1:8080")
    #debug.serve_forever()

    wf.wait()
    wf.finish()

    print("Success")