import os
from tempfile import NamedTemporaryFile
from .utils.nitertools import sort_iters, unique_sorted
import sqlite3

from .EtlRecord import EtlRecord

class EtlRecordShelf:
    '''
    Class that can store large number of records efficiently

    Records are stored with an assigned key.  Multiple records can have
    the same key, and when retrieved all records that have a given key
    will be returned in a random'ish order.
    '''

    def __init__(self, session, limit=None):
        self.__mem_records = dict() # [key] = list(record, ...)
        self.__mem_count = 0
        self.session = session
        self.__disk_store_path = None
        self.__disk_store_db = None
        self.__limit = limit


    def _normalize_key(self, key):
        if key.__class__ is int:
            return 'i:%s' % ''.join(reversed(str(key))) # reversing key string to make sortable
        elif key.__class__ is str:
            return 's:'+key
        raise ValueError("Key needs to be either str or int")


    def _denormalize_key(self, key):
        if key.startswith('i:'):
            return int(''.join(reversed(key[2:])))
        elif key.startswith('s:'):
            return key[2:]
        else:
            raise ValueError("Invalid normalized key: " + key)


    def add(self, key, record):
        '''
        Add record to the shelf

        :param key: key to use to retrieve the record
        :param record: record to be saved
        '''
        key = self._normalize_key(key)

        if key not in self.__mem_records:
            self.__mem_records[key] = list()
        self.__mem_records[key].append(record)
        self.__mem_count += 1

        self.check_limit()


    def has(self, key):
        '''
        Check to see if shelf has any records for a given the key

        :param key: key value that was passed to add()
        :return: boolean
        '''
        key = self._normalize_key(key)

        # Memory
        if key in self.__mem_records:
            if len(self.__mem_records[key]) > 0:
                return True

        # Disk
        if self.__disk_store is not None:
            results = self.__disk_store.cursor().execute("""\
                select count(*) as cnt
                from v_group_keys
                left join group_keys g on g.id = r.group_id
                where g.group_key = ?
              """, (key))
            for row in results:
                if row[0] > 0:
                    return True

        return False


    def keys(self):
        '''Return all of the the keys that exist in the shelf in sorted order'''

        def _mem_keys():
            for mk in sorted(self.__mem_records):
                if len(self.__mem_records[mk]) > 0:
                    yield self._denormalize_key(mk)

        def _disk_keys():
            if self.__disk_store is not None:
                results = self.__disk_store.cursor().execute("""\
                  select group_key
                  from v_group_keys
                  order by group_key
                  """)
                for row in results:
                    yield self._denormalize_key(row[0])

        return unique_sorted(sort_iters(_mem_keys(), _disk_keys()))


    def get(self, key, keep=False):
        '''
        Retrieve all records stored under the key

        :param key: key value that was passed to add()
        :param keep: If ture, leave records in the shelf
        :return: records returned retrieved back from shelf aftger add()
        '''
        key = self._normalize_key(key)
        any = False

        # Memory
        try:
            for record in self.__mem_records[key]:
                yield record
                any = True
        except KeyError:
            pass

        # Disk
        if self.__disk_store is not None:
            results = self.__disk_store.cursor().execute("""\
                select
                  rectype,
                  id as rec_id,
                  data
                from v_records
                where group_key = ?
                """, (key, ))
            for row in results:
                yield EtlRecord.restore(
                    rec_type = row[0],
                    serial = row[1],
                    data = row[2],
                    attr_handler = self.session.attribute_handler,
                )

        if not any:
            raise KeyError("No records stored under key " + self._denormalize_key(key))

        if not keep:

            # Memory
            if key in self.__mem_records:
                self.__mem_count -= len(self.__mem_records[key])
                del self.__mem_records[key]

            # Disk
            if self.__disk_store is not None:
                self.__disk_store.cursor().execute("""\
                    delete from records
                    where group_id in (select g.id from group_keys g where g.group_key = ?)
                    """, key)
                self.__disk_store.commit()


    def all(self, keep=True):
        '''
        Return all items from the self

        :param keep: If ture, leave records in the shelf
        '''

        # TODO: Allow returned records to be removed?  Or just returned keys?
        # TODO: Write more efficient methods to get in one select?

        # Get records
        for key in self.keys():
            for rec in self.get(key, keep=True):
                yield rec

        # Remove returned records
        if not keep:
            self.__mem_records = dict()
            self.__mem_count = 0

            if self.__disk_store is not None:
                self.__disk_store.cursor().execute("""\
                    delete from records
                    """)
                self.__disk_store.commit()


    def check_limit(self):
        '''Check to see if the memory limit has been hit'''
        if self.__mem_count > self.__limit:
            self.dump_to_disk()


    @property
    def __disk_store(self):

        # Calculate path to store records on disk
        created = False
        if self.__disk_store_path is None:
            self.__disk_store_path = NamedTemporaryFile(
                delete=False,
                dir=self.session.temp_directory,
                suffix='.shelf')
            self.__disk_store_path.close()
            self.__disk_store_path = self.__disk_store_path.name
            created = True

        # Create DB if doesn't exist
        if self.__disk_store_db is None:
            if created:
                os.unlink(self.__disk_store_path)
                self.__disk_store_db = sqlite3.connect(self.__disk_store_path)

                self.__disk_store_db.cursor().execute("""\
                    create table load (
                      ld_rec_id        integer,
                      ld_group_id      text,
                      ld_rec_type      text,
                      ld_data          text
                    )
                    """)

                self.__disk_store_db.cursor().execute("""\
                    create table group_keys (
                      id        integer PRIMARY KEY,
                      group_key text
                    )
                    """)

                self.__disk_store_db.cursor().execute("""\
                    create table record_types (
                      id        integer PRIMARY KEY,
                      rectype   text
                    )
                    """)

                self.__disk_store_db.cursor().execute("""\
                    create table records (
                      id        integer PRIMARY KEY,    -- Serial
                      group_id  integer   not null,
                      type_id   integer   not null,
                      data      text      not null,     -- Data from EtlRecord.store()
                      FOREIGN KEY (group_id) REFERENCES group_keys(id),
                      FOREIGN KEY (type_id) REFERENCES record_types(id)
                    )
                    """)

                self.__disk_store_db.cursor().execute("""\
                    create view v_group_keys as
                    select
                      g.group_key,
                      count(r.id) as cnt
                    from group_keys g
                    left join records r on r.group_id = g.id
                    group by g.group_key
                    having count(r.id) > 0
                    """)

                self.__disk_store_db.cursor().execute("""\
                    create view v_records as
                    select
                      t.rectype,
                      r.id,
                      g.group_key,
                      r.data
                    from records r
                    left join group_keys g on g.id = r.group_id
                    left join record_types t on t.id = r.type_id
                    """)

            else:
                raise Exception("Not sure it's valid to ever open an existtng DB")

        # Return DB
        return self.__disk_store_db


    def dump_to_disk(self):
        '''Write the records in memory to disk to free up memory'''

        print("dumping")

        # Load records into DB load table
        cursor = self.__disk_store.cursor()

        for group_key in self.__mem_records:
            for record in self.__mem_records[group_key]:
                rectype, serial, data = record.store()
                cursor.execute("""\
                    INSERT INTO load (ld_rec_id, ld_group_id, ld_rec_type, ld_data)
                    VALUES (?, ?, ?, ?)
                    """, (int(serial), str(group_key), rectype, data))

        self.__disk_store.commit()

        # Move data into normalized tables
        cursor.execute("""\
                INSERT INTO group_keys (group_key)
                    select distinct ld_group_id
                    from load
                    left join group_keys g on ld_group_id = group_key
                    where g.id is null;
                """)

        cursor.execute("""\
                INSERT INTO record_types (rectype)
                    select distinct ld_rec_type
                    from load
                    left join record_types rt on rectype = ld_rec_type
                    where rt.id is null;
                """)

        cursor.execute("""\
                INSERT INTO records (id, group_id, type_id, data)
                    select
                      ld_rec_id, g.id, rt.id, ld_data
                    from load
                    left join group_keys g on g.group_key = ld_group_id
                    left join record_types rt on rt.rectype = ld_rec_type
                """)

        cursor.execute("delete from load")

        self.__disk_store.commit()

        # Get ready for more mem records
        self.__mem_records = dict()
        self.__mem_count = 0



