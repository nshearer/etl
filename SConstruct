#!/bin/scons
'''
SConstruct - Build instructions for SBC Database

'''

import os
import sys
import re
import shutil
import subprocess
from fnmatch import fnmatch

python = sys.executable

# -- Constants ---------------------------------------------------------------



# -- File finding tools ------------------------------------------------------

def find_files(search, exts=None):
    for dirpath, dirnames, filenames in os.walk(search):
        for filename in filenames:
            name, ext = os.path.splitext(filename)
            if exts is None or ext[1:].lower() in exts:
                yield os.path.join(dirpath, filename)


# -- Setup scons Environment -------------------------------------------------

env = Environment(
    tools=[],
    BUILDERS = {
        'HtmlAssets': Builder(action = "{python} {script} {source} $TARGET" .format(
            python = python,
            script = 'src/compile_netl_anaylze_assets.py',
            source = 'src/netl_analyze/html')),
        }
    )

# -- Start Building ----------------------------------------------------------

# HTML assets for NETL Analyze
netl_analyze_files = list(find_files('src/netl_analyze/html'))
netl_analyze_html = env.HtmlAssets('src/netl_analyze/html_content.py', netl_analyze_files)
