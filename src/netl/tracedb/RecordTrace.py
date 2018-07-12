from pprint import pformat
from hashlib import md5
import struct

from .TraceObject import TraceData, TraceAction


def repr_attr_value(value):
    '''
    Create a representation of the value of an attribute for the TraceDB

    Since the TraceDB is only used for monitoring and debugging, this needs
    to turn each value into a string a person can understand.

    Also will return a hash to index the value with for checking the DB
    to see if value is aleady there.

    :param value:
    :return: str, value_hash
    '''

    if value is None:
        return None, None

    s = pformat(value).strip("'")

    # sqlite supports up to 62 bits.  Using 32 bit hash (4 bytes)
    h = struct.unpack("L", md5(s.encode('utf-8')).digest()[:4])[0]

    return s, h


class RecordTrace(TraceData):
    '''A record in the ETL'''

    TABLE = 'records'

    CREATE_STATEMENTS = (
        """\
            create table records (
              id                  int primary key,
              rectype             text,
              origin_component_id int)
        """,
        """\
            create table record_attributes (
              rec_id              int,
              attr_name           text,
              sort                int,
              value_hash          int)
        """,
        """\
            create table record_data (
              value_hash          int primary key,
              value               text)
        """,
        """\
            create view v_record_attribues as
            select
              r.id                      rec_id,
              r.rectype                 rectype,
              r.origin_component_id     origin_comp_id,
              a.attr_name               attr_name,
              d.value                   value
            from records r
            left join record_attributes a on a.rec_id = r.id
            left join record_data d on d.value_hash = a.value_hash
            order by r.id, a.sort
        """,
        """\
            create view v_record_attribute_names as
            select
              rectype,
              attr_name
            from records r
            left join record_attributes a on r.id = a.rec_id
            group by rectype, attr_name
            order by max(a.sort)
        """,
    )


class TraceRecord(TraceAction):
    '''Save record to TraceDB'''


    def __init__(self, record):
        '''
        :param from_port_id: Unique integer ID of the output port being
        :param to_port_id: Unique integer ID for the input port
        '''
        super(TraceRecord, self).__init__()
        self.record = record


    def record_trace_to_db(self, trace_db, commit):

        # Record Record
        trace_db.execute_update("""
            insert into records (id, rectype, origin_component_id)
            values (?, ?, ?)
            """, (
                int(self.record.serial),
                self.record.record_type,
                int(self.record.origin_component_id)))

        # Put attribute values into the DB
        pos = -1
        for key, value in self.record.attributes:
            pos += 1

            # Record attribute value

            value_repr, value_hash = repr_attr_value(value)

            exist = trace_db.execute_count("""\
                select count(*) from record_data
                where value_hash = ?
                """, (value_hash, ))

            if exist == 0:
                trace_db.execute_update("""
                    insert into record_data (value_hash, value)
                    values (?, ?)
                    """, (value_hash, value_repr))

            # Associate record attribute with value

            trace_db.execute_update("""
                insert into record_attributes (rec_id, attr_name, value_hash, sort)
                values (?, ?, ?, ?)
                """, (int(self.record.serial),
                      key,
                      value_hash,
                      pos))


        if commit:
            trace_db.commit()


