import os
import csv

from .. import EtlComponent
from .MultiPassInput import MultiPassInput

class WriteCSVFile(EtlComponent):
    '''Component to write data out to a CSV file'''

    records_to_write = MultiPassInput()

    def __init__(self, path, headers=None, extra_headers=None):
        '''
        :param path: Path to CSV to write
        :param headers:
            List of tuple(attr_name, header)
            Header to write.
            Each item in list is the attribute name to read from the record and the header to place
            in the column
        :param extra_headers:
            List of lists
            Extra rows to write at the top of the file
        '''

        super(WriteCSVFile, self).__init__()

        self.__path = os.path.abspath(path)

        if headers is not None:
            try:
                self.__headers = [(attr_name, header) for (attr_name, header) in headers]
            except ValueError:
                raise Exception("headers needs to be a list of (attr_name, header)")
        else:
            self.__headers = None

        self.__extra_headers = extra_headers


    def run(self):

        # Open output file
        with open(self.__path, "wt", newline="") as fh:
            writer = csv.writer(fh)

            # Write out extra headers
            if self.__extra_headers is not None:
                for row in self.__extra_headers:
                    try:
                        writer.writerows(self.__extra_headers)
                    except Exception as e:
                        raise Exception("Extra headers format invalid: " + str(e))

            # Get column names from all input records to determine header
            if self.__headers is None:
                self.__headers = list()
                seen = set()
                for rec in self.records_to_write.all(envelope=False):
                    for attr_name in rec.attr_names:
                        if attr_name not in seen:
                            self.__headers.append((attr_name, attr_name))
                            seen.add(attr_name)

            # Write coumn headers
            writer.writerow([attr_name for (attr_name, header) in self.__headers])

            # Write out records
            for rec in self.records_to_write.all(envelope=False):
                row = list()
                for attr_name, header in self.__headers:
                    try:
                        row.append(rec[attr_name] or "")
                    except KeyError:
                        row.append("")
                writer.writerow(row)

            writer = None

        # Cleanup
        self.records_to_write.done()
