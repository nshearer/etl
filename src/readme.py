#!/usr/bin/python
'''This script is used to build README.md from readme.tpl'''

import os
import sys
from textwrap import dedent

from etl.EtlWorkflow import EtlWorkflow
from etl.EtlProcessorBase import EtlProcessorBase
from etl.EtlProcessor import EtlProcessor
from etl.schema.EtlSchema import EtlSchema

def abort(msg):
    print "ERROR:", msg
    sys.exit(2)


def get_class_doc(class_obj):
    doc = class_obj.__doc__.split("\n")

    # Pad first line with spaces to help with dedent
    doc[0] = ' '*4 + doc[0]

    # Pad blank lines with spaces for dedent
    for i, line in enumerate(doc):
        if len(line.strip()) == 0:
            doc[i] = ' '*8

    # Dedent
    doc = dedent("\n".join(doc))

    return doc


if __name__ == '__main__':

    tpl_path = os.path.join(os.path.dirname(__file__), '..', 'readme.tpl')
    tpl_path = os.path.abspath(tpl_path)
    if not os.path.exists(tpl_path):
        abort("Template file missing: " + tpl_path)


    tgt_path = os.path.join(os.path.dirname(__file__), '..', 'README.md')
    tgt_path = os.path.abspath(tgt_path)

    tpl = open(tpl_path, 'rt').read()

    tpl = tpl.replace('[include:EtlWorkflow]', get_class_doc(EtlWorkflow))
    tpl = tpl.replace('[include:EtlProcessorBase]', get_class_doc(EtlProcessorBase))
    tpl = tpl.replace('[include:EtlProcessor]', get_class_doc(EtlProcessor))
    tpl = tpl.replace('[include:EtlSchema]', get_class_doc(EtlSchema))

    print "Writing", tgt_path
    open(tgt_path, 'wt').write(tpl)
