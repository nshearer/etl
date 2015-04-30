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


def get_class_head_doc(class_obj):
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


class MethodDoc(object):
    def __init__(self, doc):
        self.doc = doc
        self.header = None
        self.body = None
        self.parms = dict()
        self.parm_order = list()
        self._parse()
    def _parse(self):
        self.header = None
        self.body = None
        self.parms = dict()
        self.parm_order = list()
        if self.doc is None:
            return
        lines = self.doc.split("\n")
        # Header
        if len(lines) > 0:
            self.header = lines[0].strip()
        # Body & parms
        in_parm = None
        for line in lines[2:]:
            line = line[8:].rstrip()
            # Detect variables
            if '@param' in line or '@parm' in line:
                parts = line.split(' ')
                in_parm = parts[1].rstrip(':')
                self.parms[in_parm] = ' '.join(parts[2:])
                self.parm_order.append(in_parm)
            # In variable decleration
            elif in_parm is not None:
                self.parms[in_parm] += "\n" + line
            # In body
            else:
                if self.body is None:
                    self.body = line
                else:
                    self.body += "\n" + line


def deslash(name):
    return name.replace('_', '\_')


def get_class_method_doc(class_obj):
    '''List the methods of a class object to assist with documenting key classes'''
    doc = list()

    # List Methods
    doc.append("Methods of %s" % (class_obj.__name__))
    doc.append("------------------------------------")
    doc.append("")
    for method_name in dir(class_obj):
        method = getattr(class_obj, method_name)
        if type(method).__name__ == 'instancemethod':
            if method_name[0] != '_':
                method_doc = MethodDoc(method.__doc__)
                doc.append('**%s(%s)**:' %(
                    deslash(method_name),
                    ', '.join([deslash(name) for name in method_doc.parm_order])))
                if method_doc.header is not None:
                    doc.append('')
                    doc.append('%s' % (method_doc.header))
                if method_doc.body is not None:
                    doc.append('')
                    doc.append(method_doc.body.rstrip())
                if len(method_doc.parm_order) > 0:
                    doc.append('')
                    for parm in method_doc.parm_order:
                        doc.append(' - **%s**: %s' % (
                            deslash(parm),
                            method_doc.parms[parm]))

                doc.append('')

        # else:
        #     print "skipping", type(method).__name__

    return "\n".join(doc)



if __name__ == '__main__':

    tpl_path = os.path.join(os.path.dirname(__file__), '..', 'readme.tpl')
    tpl_path = os.path.abspath(tpl_path)
    if not os.path.exists(tpl_path):
        abort("Template file missing: " + tpl_path)


    tgt_path = os.path.join(os.path.dirname(__file__), '..', 'README.md')
    tgt_path = os.path.abspath(tgt_path)

    tpl = open(tpl_path, 'rt').read()

    tpl = tpl.replace('[include:EtlWorkflow]', get_class_head_doc(EtlWorkflow))
    tpl = tpl.replace('[include:EtlWorkflow:methods]', get_class_method_doc(EtlWorkflow))


    tpl = tpl.replace('[include:EtlProcessorBase]', get_class_head_doc(EtlProcessorBase))
    tpl = tpl.replace('[include:EtlProcessor]', get_class_head_doc(EtlProcessor))
    tpl = tpl.replace('[include:EtlSchema]', get_class_head_doc(EtlSchema))

    print "Writing", tgt_path
    open(tgt_path, 'wt').write(tpl)
