import os
from tempfile import NamedTemporaryFile
from .utils.nitertools import ngroup
import sqlite3

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


    def add(self, key, record):
        '''
        Add record to the shelf

        :param key: key to use to retrieve the record
        :param record: record to be saved
        '''
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
        return key in self.__mem_records


    def keys(self):
        return self.__mem_records.keys()


    def get(self, key, keep=False):
        '''
        Retrieve all records stored under the key

        :param key: key value that was passed to add()
        :param keep: If ture, leave records in the shelf
        :return: records returned retrieved back from shelf aftger add()
        '''

        try:
            for record in self.__mem_records[key]:
                yield record
        except KeyError:
            pass


    def all(self, sort_keys=False):
        '''
        Return all items from the self

        :param sort_keys: Return items in sorted key order
        '''

        if sort_keys:
            for key in sorted(self.keys()):
                for record in self.__mem_records[key]:
                    yield record
        else:
            for key in self.__mem_records:
                for record in self.__mem_records[key]:
                    yield record



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

            else:
                raise Exception("Not sure it's valid to ever open an existtng DB")

        # Return DB
        return self.__disk_store_db


    def dump_to_disk(self):
        '''Write the records in memory to disk to free up memory'''

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

        # Get ready for more mem records
        self.__mem_records = dict()
        self.__mem_count = 0



