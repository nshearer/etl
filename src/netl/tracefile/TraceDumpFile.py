
try:
    import ujson as json
except ImportError:
    import json


from ..utils.GZOutputWriter import GZOutputWriter, GZOutputReader

class TraceDumpFileWriter:
    '''
    Dumps activity information to a file to examine how the ETL ran

    File Format:

        The dump file is a zlib compressed text file

         Each item is written to the file as a set of lines:

            RECORD_HEADER: R CODE Length
            RECORD_DATA

        Example:

            R LOG 60
            {'severity': 'INFO', 'msg': 'Starting component "Extract"'}

        A newline is always added to the end of the record data so that the file
        can be efficiently read back as a set of lines

    '''

    VERSION='dev'

    def __init__(self, path):
        self.__fh = GZOutputWriter(self.__path)
        self.__new_data = False


    def write(self, entry_code, data):
        '''
        Add data to the file

        :param entry_code: A string to identify the type of entry being saved
        :param data: The data to be saved.  Must be json serializable
        '''

        data = json.dumps(data).encode('utf-8') + b"\n"

        header = "R %s %d\n" % (entry_code, len(data))
        header = header.encode('utf-8')

        self.__fh.write(header)
        self.__fh.write(data)
        self.__new_data = True


#     def add_trace_header(self, trace_port):
#         '''
#         Information about the running process so that monitor apps can connect
#
#         :param trace_port: Port the ETL program is listening for status requests on
#         '''
#         self.add(
#             "TRACER",
#             {
#                 'port': int(trace_port),
#             }
#         )
#
#
#     def add_log_msg(self, logger_name, severity, msg):
#         '''
#         Save a log message
#
#         :param logger_name: Name of the logger being used
#         :param severity: Message severity
#         :param msg: Actual message
#         '''
#         self.add(
#             "LOG",
#             {
#                 'name': logger_name,
#                 'severity': severity,
#                 'msg': msg,
#             }
#         )
#
#     def add_record(self, record):
#         '''
#         Save a record
#
#         :param record: EtlRecord
#         '''
#         self.add(
#             "REC",
#             {
#                 'type': record.record_type,
#                 'serial': str(record.serial),
#                 'attrs': {k: v for (k, v) in record.repr_attrs()},
#             }
#         )
#
#     def add_record_sent(self, envl, from_comp_id, to_comp_id):
#         '''
#         Note that a record was sent out of a component
#
#         :param envl: EtlEnvilope with message dispatch details
#         :param from_comp_id:
#         '''
#         self.add(
#             "ENVL",
#             {
#                  'msg_type': envl.msg_type,
#                  'from_comp_name': envl.from_comp_name,
#                  'from_comp_id': envl.from_comp_id,
#                  'from_port_id': envl.from_port_id,
#                  'from_port_name': envl.from_port_name,
#                  'record_id': str(envl.record.serial),
#                  'to_comp_name': envl.to_comp_name,
#                  'to_comp_id': envl.to_comp_id,
#                  'to_port_name': envl.to_port_name,
#                  'to_port_id': envl.to_port_id,
#                  'to_comp_name': envl.to_comp_name,
#                  'to_comp_id': envl.to_comp_id,
#                  'to_port_name': envl.to_port_name,
#                  'to_port_id': envl.to_port_id,
#             }
#         )
#
#
#     def flush(self):
#         '''Make sure written data is flushed out to disk'''
#         if self.__new_data:
#             self.__fh.flush()
#             self.__new_data = False


class TraceDumpFileReader:
    '''
    Reads back items written by TraceDuempFileWriter
    '''

    def __init__(self, path):
        self.__path = path


    def all(self, entry_code, data):
        '''
        Reads through all the items and yields them back

        Note:
         - reopens the file each time, so you can call multiple times
         - gracefully handles end of file, so you can read it while it's
           being written out elsewhere.

        :return: yeilds item_code, item_data
        '''

        fh = GZOutputReader(self.__path)

        parse_except = None
        item_type_code = None
        data_len = None
        data = None
        for i, line in enumerate(fh.readlines()):
            try:

                # Process header
                if item_type_code is None:
                    try:
                        line_code, item_type_code, data_len = line.strip().split(" ")
                    except ValueError:
                        raise Exception("Couldn't parse header line %d" % (i+1))
                    if line_code != 'R':
                        raise Exception("Line %d should be header, but doesn't start with R" % (i+1))
                    data = list()

                # Process data
                else:
                    data.append(line)
                    collected_bytes = sum([len(l) for l in data])
                    if collected_bytes == data_len:
                        data = json.loads(b"".join(data))
                        yield item_type_code, data
                        item_type_code = None
                        data = None
                    elif collected_bytes > data_len:
                        raise Exception("While gathering data line %d, got %d bytes, but expected %d" % (
                            i+1, collected_bytes, data_len
                        ))

            except Exception as e:
                # Is this eception because we reached the end of the file?
                if parse_except is not None:
                    raise parse_except
                parse_except = e

