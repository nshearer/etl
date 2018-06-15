
class MemoryRecordSet(object):
    '''Stores records in memory.
    
    Don't use this class directly, but use EtlRecordSet instead
    '''
    
    def __init__(self):
        self.__records = dict()
        self.__tags = dict()
        self.__record_tags = dict()
        self.__size = 0
        
    
    def dump_records(self):
        '''Return all records
        
        @return: Generator of (record, tags)
        '''
        for serial in self.__records:
            record = self.__records[serial]
            tags = list()
            if serial in self.__record_tags:
                tags = list(self.__record_tags[serial])
            
            yield record, tags
    
    
    def add_record(self, etl_rec, tags=None):
        '''Add a record to the collection
        
        Use indexes to index this 
        
        @param etl_rec: Record to store
        @param tags: List of optional additional tags to be used for retrieving
            this record.  Record must be convertable to a string with str()
        '''
        # Check Duplicate
        if etl_rec.serial in self.__records:
            msg = etl_rec.create_msg("Record already in record set")
            raise IndexError(msg)
        
        if not etl_rec.is_frozen:
            raise Exception("Cannot add non-frozen record")
        
        # Add Record
        self.__records[etl_rec.serial] = etl_rec
        
        # Save Tag Values
        if type(tags) is str:
            tags = [tags, ]
        if tags is not None:
            self.__record_tags[etl_rec.serial] = set(tags)
            for tag in tags:
                if tag not in self.__tags:
                    self.__tags[tag] = set()
                self.__tags[tag].add(etl_rec.serial)
                
        # Update Size
        self.__size += etl_rec.size
        
        
        
    def get_record(self, serial):
        '''Retrieve a record
        
        @param serical:
            Record identifier
        @return EtlRecord
        '''
        return self.__records[serial]
    
    
    def has_record(self, serial):
        return serial in self.__records
    
    
    def find_records_with_tag(self, tag):
        '''Find records that have a given tag'''
        if tag in self.__tags:
            for serial in self.__tags[tag]:
                yield self.get_record(serial)
                
                
    def has_record_with_tag(self, tag):
        if tag in self.__tags:
            if len(self.__tags[tag]) > 0:
                return True
        return False
                
                
    def remove_record(self, serial):
        '''Drop a record from the collection'''
        # Deduct size
        self.__size -= self.__records[serial].size
        
        # Remove record
        del self.__records[serial]
        
        # Clean up tags
        if serial in self.__record_tags:
            for tag in self.__record_tags[serial]:
                self.__tags[tag].remove(serial)
                if len(self.__tags[tag]) == 0:
                    del self.__tags[tag]
            del self.__record_tags[serial]
            
        
    @property
    def size(self):
        '''Estimated size of the record set'''
        return self.__size
    
    
    @property
    def count(self):
        return len(self.__records)
    
    
            
        