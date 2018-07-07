import os, sys
from zipfile import ZipFile, ZIP_BZIP2
from tempfile import TemporaryFile
from base64 import b64encode
from textwrap import dedent

if __name__ == '__main__':

    # Parse args
    try:
        html_path, target_path = sys.argv[1:]
        if not os.path.exists(html_path) or not os.path.isdir(html_path):
            print("%s doesn't exist" % (html_path))
            raise Exception()
    except:
        print("USAGE: %s html_source_path py_target_path" % (os.path.basename(sys.argv[0])))
        sys.exit(1)

    # Zip up the files
    zip_fh = TemporaryFile(suffix='.zip')
    with ZipFile(zip_fh, mode='w') as zip:
        for dirpath, dirnames, filenames in os.walk(html_path):
            for filename in filenames:
                path = os.path.join(dirpath, filename)
                name = path[len(html_path)+1:]
                print(name)
                zip.write(path, name, ZIP_BZIP2)
    print()

    # Encapsulate data into a Python Module
    zip_fh.seek(0)
    b64zip = b64encode(zip_fh.read()).decode('ascii')

    chunk_size = 100
    b64zip = [b64zip[i:i+chunk_size] for i in range(0, len(b64zip), chunk_size)]

    print("Writing " + target_path)
    with open(target_path, 'wt') as fh:
        fh.write(dedent("""\
            from base64 import b64decode
            from io import BytesIO
            from zipfile import ZipFile
            
            _NETL_ANALYZE_ZIP_BINARY = BytesIO(b64decode('''
            {b64zip}
                '''.strip().replace("\\n", "")))
            NETL_ANALYZE_HTML = ZipFile(_NETL_ANALYZE_ZIP_BINARY, 'r')
            """).format(
                b64zip = "\n".join(['    ' + line for line in b64zip])
            ))

