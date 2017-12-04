import os
from tempfile import NamedTemporaryFile
import sqlite3
import cPickle

class Sqlite3RecordSet(object):
    '''Stores records into an sqlite3 database.
    
    Don't use this class directly, but use EtlRecordSet instead
    '''
    
    def __init__(self):
        self.__path = NamedTemporaryFile(delete=False).name
        self.__db = sqlite3.connect(self.__path)
        self.__schema_ids_by_key = dict()
        self.__schemas_by_id = list()
        
        self._init_db()
        
        self.__size = 0
        
        
    def __del__(self):
        self.__db.close()
        os.unlink(self.__path)
        self.__path = None
        
    
    def _init_db(self):
        '''Create database tables to hold records'''
        curs = self.__db.cursor()
        
        curs.execute('''
            CREATE TABLE records (
                serial     text     primary key,
                record     blob,
                schema_id  int)
            ''')
        
        curs.execute('''
            CREATE TABLE tags (
                tag        text,
                serial     text)
            ''')
    
        curs.execute('''
            CREATE INDEX tag_index ON tags (tag)
            ''')
        
        self.__db.commit()
        
        
    def add_record(self, etl_rec, tags=None):
        '''Add a record to the collection
        
        Use indexes to index this 
        
        @param etl_rec: Record to store
        @param tags: List of optional additional tags to be used for retrieving
            this record.  Record must be convertable to a string with str()
        '''
        if not etl_rec.is_frozen:
            raise Exception("Cannot add non-frozen record")
        
        # Extract schema to save pickled object size
        schema_id = self._save_schema(etl_rec.schema)
        etl_rec.set_schema(None) 
        
        # Pickle the object and save
        record_data = cPickle.dumps(etl_rec, cPickle.HIGHEST_PROTOCOL)
        curs = self.__db.cursor()
        curs.execute("""\
            insert into records (serial, record, schema_id)
            values (?, ?, ?)
            """,
            (str(etl_rec.serial), sqlite3.Binary(record_data), schema_id))
        
        # Save Tag Values
        if tags is not None:
            for tag in tags:
                curs.execute("""\
                insert into tags (tag, serial)
                values (?, ?)
                """,
                (str(tag), str(etl_rec.serial)))
                
        self.__db.commit()
            
        # Update Size
        self.__size += etl_rec.size
        
        
    def get_record(self, serial):
        '''Retrieve a record
        
        @param serial: Record identifier
        @return EtlRecord
        '''
        # Retrieve record
        curs = self.__db.cursor()
        results = curs.execute("""\
            SELECT record, schema_id
            FROM records
            WHERE serial = ?
            """, (str(serial), ))
        for row in results:
            return self._rebuild_record(str(row[0]), int(row[1]))
        
        # Not Found
        raise IndexError("Record does not exist: " + str(serial))
    
    
    def _rebuild_record(self, record_data, schema_id):
        # Unpickle record
        record = cPickle.loads(record_data)
        
        # Restore schema
        schema = self._get_stored_schema(schema_id)
        record.set_schema(schema)
        
        return record
    
    
    def has_record(self, serial):
        curs = self.__db.cursor()
        results = curs.execute("""\
            SELECT count(*)
            FROM records
            WHERE serial = ?
            """, (str(serial), ))
        if int(results.fetchone()[0]) > 0:
            return True
        return False
            
    
    def find_records_with_tag(self, tag):
        '''Find records that have a given tag'''
        curs = self.__db.cursor()
        
        # Collect serial numbers for tag
        results = curs.execute("""\
            SELECT records.record, records.schema_id
            FROM tags
            LEFT JOIN records on tags.serial = records.serial
            WHERE tags.tag = ?
            """, (tag, ))
        
        # Return back records
        for row in results:
            yield self._rebuild_record(str(row[0]), int(row[1]))
        
                
    def has_record_with_tag(self, tag):
        curs = self.__db.cursor()
        results = curs.execute("""\
            SELECT count(*)
            FROM tags
            LEFT JOIN records on tags.serial = records.serial
            WHERE tags.tag = ?
            """, (tag, ) )
        if int(results.fetchone()[0]) > 0:
            return True
        return False
                
                
    def remove_record(self, serial):
        '''Drop a record from the collection'''
        # Deduct size
        record = self.get_record(serial)
        self.__size -= record.size
        
        # Remove records
        curs = self.__db.cursor()
        curs.execute("DELETE FROM records WHERE serial = ?", (str(serial), ))
        curs.execute("DELETE FROM tags WHERE serial = ?", (str(serial), ))
        self.__db.commit()
        
        
    @property
    def size(self):
        '''Estimated size of the record set'''
        return self.__size
    
    
    @property
    def count(self):
        curs = self.__db.cursor()
        results = curs.execute("""\
            SELECT count(*)
            FROM records
            """)
        return int(results.fetchone()[0])
    
    
    def _save_schema(self, schema):
        '''Save schema into memory'''
        # Create key based on schema name
        schema_key = schema.__class__.__name__
        if not self.__schema_ids_by_key.has_key(schema_key):
            self.__schema_ids_by_key[schema_key] = list()
        
        # Check to see if we already have this schema stored
        for stored_schema_id in self.__schema_ids_by_key[schema_key]:
            if self.__schemas_by_id[stored_schema_id] == schema:
                return stored_schema_id
        
        # Else, save schema
        schema_id = len(self.__schemas_by_id)
        self.__schemas_by_id.append(schema)
        self.__schema_ids_by_key[schema_key].append(schema_id)
        
        return schema_id
        
        
    def _get_stored_schema(self, schema_id):
        return self.__schemas_by_id[schema_id]
    
    
    @property
    def db_path(self):
        return self.__path
        
    

        