from MemoryRecordSet import MemoryRecordSet
from Sqlite3RecordSet import Sqlite3RecordSet

# class EtlRecordSubSet(object):
#     '''Used to group records'''
#     
#     def __init__(self):
#         self.__indexes = dict()
#         self.__records = list()
#         self.__on_disk = False 
#         self.__disk_file = None
#         self.__record_count = 0
#         
#         
#     @property
#     def on_disk(self):
#         return self.__on_disk
#     
#     
#     @property
#     def record_count(self):
#         return self.__record_count
#     
#     
#     # -- Adding Records -------------------------------------------------------
# 
#     def append(self, record):
#         '''Add a record to the subset'''
#         
#         # Make sure set is loaded
#         if self.__on_disk:
#             raise Exception("Cannot add to subset after saving to disk")
#             
#         # Add record
#         self.__records.append(record)
#         if record.index is not None:
#             self.__indexes[record.index] = record
#             
#         self.__record_count += 1
#             
#             
#     def has_index(self, idx):
#         if self.__on_disk:
#             return idx in self.__indexes        # Set
#         else:
#             return self.__indexes.has_key(idx)  # Dict
#         
#         
#     # -- Saving to Disk -------------------------------------------------------
#         
#     def move_to_disk(self):
#         '''Move this set of records to disk to save memory'''
#         if self.__on_disk:
#             return
#         
#         # Get a file to store it
#         self.__disk_file = TemporaryFile();
#         
#         # Dump records
#         pickle.dump((self.__records, self.__indexes), self.__disk_file)
#         
#         # Convert indexes to a set
#         self.__indexes = set(self.__indexes.keys())
#         
#         self.__on_disk = True
# 
#     
#     def _retrieve_records_from_disk(self):
#         '''Retrieve the list of records that were saved to disk'''
#         if not self.__on_disk:
#             raise Exception("Subset not saved to disk")
#         
#         self.__disk_file.seek(0)
#         saved = pickle.load(self.__disk_file)
#         return saved[0]
#         
# 
#     def _retrieve_records_by_index_from_disk(self):
#         '''Retrieve the list of records that were saved to disk'''
#         if not self.__on_disk:
#             raise Exception("Subset not saved to disk")
#         
#         self.__disk_file.seek(0)
#         saved = pickle.load(self.__disk_file)
#         return saved[1]
#         
# 
#     # -- Accessing records ---------------------------------------------------
# 
#     def all_records(self):
#         '''Retrieve all records in the sub-set'''
#         if self.__on_disk:
#             for record in self._retrieve_records_from_disk():
#                 yield record
#         else:
#             for record in self.__records:
#                 yield record
#                 
#                 
#     def get_record(self, index):
#         '''Retrieve a single record by it's index'''
#         if self.__on_disk:
#             by_index = self._retrieve_records_by_index_from_disk()
#             return by_index[index]
#         else:
#             return self.__indexes[index]
#         

class EtlRecordSet(object):
    '''Encapsulate a set of records
    
    This object can be used to store multiple records.  If the volume stays
    small (under a configurable limit) the records will be stored in memory.
    If, however, the number of records based on estimated record size, then
    the records will be moved off to disk.
    
    Additionally, indexes can be added for retrieving records by values other
    than the record serial
    '''
    
    MAX_SIZE_BEFORE_DISK = 10000
    
    def __init__(self, src_prc_name, output_name, schema):
        self.src_prc_name = src_prc_name
        self.output_name = output_name
        
        self.__store = MemoryRecordSet()
        self.__on_disk = False
    
    
    def add_record(self, etl_rec, tags=None):
        '''Add a record to the collection
        
        Use indexes to index this 
        
        @param etl_rec: Record to store
        @param tags: List of optional additional tags to be used for retrieving
            this record.  Record must be convertible to a string with str()
        '''
        # Consider switching to disk storage
        if not self.__on_disk:
            if self.__store.size > EtlRecordSet.MAX_SIZE_BEFORE_DISK:
                self.convert_to_disk_storage()
                
        # Add record
        try:
            self.__store.add_record(etl_rec, tags)
        except Exception, e:
            msg = etl_rec.create_msg("Failed to store record: " + str(e))
            raise Exception(msg)
        
        
    def convert_to_disk_storage(self):
        if not self.__on_disk:
            
            new_store = Sqlite3RecordSet()
            
            for record, tags in self.__store.dump_records():
                new_store.add_record(record, tags)
            
            self.__store = new_store
            
        self.__on_disk = True
        
        
    def get_record(self, serial):
        '''Retrieve a record
        
        @param serical:
            Record identifier
        @return EtlRecord
        '''
        return self.__store.get_record(serial)
    
        
    def has_record(self, serial):
        return self.__store.has_record(serial)
    
    
    def find_records_with_tag(self, tag):
        '''Find records that have a given tag'''
        for record in self.__store.find_records_with_tag(tag):
            yield record
                
                
    def has_record_with_tag(self, tag):
        return self.__store.has_record_with_tag(tag)
                
                
    def remove_record(self, serial):
        '''Drop a record from the collection'''
        return self.__store.remove_record(serial)
            
        
    @property
    def size(self):
        '''Estimated size of the record set'''
        return self.__store.size
    
    
    @property
    def count(self):
        return self.__store.count
        
#         # -- Check Record -----------------------------------------------------
#         
#         # Check matches schema
#         errors = self.__schema.check_record_struct(rec)
#         if errors is not None:
#             for error in errors:
#                 msg = "Record generated by '%s' for '%s' does not match '%s'"
#                 msg = msg % (self.src_prc_name, self.output_name,
#                              self.__schema.__class__.__name__)
#                 msg += "\n" + error
#                 raise Exception(msg)
#         
#         # Make sure index is unique
#         if rec.index is not None:
#             if self.has_index(rec.index):
#                 msg = "Duplicate record index '%s'"
#                 msg += " generated by '%s' for '%s'"
#                 msg = msg % (rec.index, self.src_prc_name, self.output_name)
#                 raise Exception(msg)
        
#         # -- Save record ------------------------------------------------------
#         
#         # Send full record sets to disk
#         max_cnt = self.max_records_per_set
#         if self.__mem_record_subset is not None:
#             if self.__mem_record_subset.record_count >= max_cnt:
#                 self.__mem_record_subset.move_to_disk()
#                 self.__disk_record_subsets.append(self.__mem_record_subset)
#                 self.__mem_record_subset = None
# 
#         # Create a new record set if needed
#         if self.__mem_record_subset is None:
#             self.__mem_record_subset = EtlRecordSubSet()
#                                 
#         # Add record to set
#         self.__mem_record_subset.append(rec)
#         
#         # -- Trace ------------------------------------------------------------
#         if self.record_count % self.max_records_per_set == 0:
#             msg = "   %s.%s: %d records"
#             msg = msg % (self.src_prc_name, self.output_name, self.record_count)
#             print msg
#         
                    
    
#     def all_records(self):
#         '''Retrieve all records in the set'''
#         for record_set in self.__disk_record_subsets:
#             for record in record_set.all_records():
#                 yield record
#         if self.__mem_record_subset is not None:
#             for record in self.__mem_record_subset.all_records():
#                 yield record
#                 
#                 
#     def get_record(self, index):
#         '''Retrieve record by index'''
#         # Check in memory set first
#         if self.__mem_record_subset is not None:
#             if self.__mem_record_subset.has_index(index):
#                 return self.__mem_record_subset.get_record(index)
#         
#         # Check sets saved to disk
#         for record_subset in self.__disk_record_subsets:
#             if record_subset.has_index(index):
#                 return record_subset.get_record(index)
#             
#         # Did not find
#         msg = "Unknown record index: '%s' (%s:%s)"
#         msg = msg % (str(index), self.src_prc_name, self.output_name)
#         raise IndexError(msg)
#     
#     
#     @property
#     def record_count(self):
#         cnt = 0
#         if self.__mem_record_subset is not None:
#             cnt += self.__mem_record_subset.record_count
#         for record_subset in self.__disk_record_subsets:
#             cnt += record_subset.record_count
#         return cnt
#     
#     
#     def has_index(self, index):
#         if self.__mem_record_subset is not None:
#             if self.__mem_record_subset.has_index(index):
#                 return True
#         for record_subset in self.__disk_record_subsets:
#             if record_subset.has_index(index):
#                 return True
#         return False
#         
#         
# #     def export_as_excel(self, path):
# #         
# #         # Init Workbook
# #         book = Workbook()
# #         data_sheet = book.add_sheet('Data')
# #         schema_sheet = book.add_sheet('Schema')
# #         
# #         # Data Header
# #         for i, field_info in enumerate(self.schema.list_fields()):
# #             data_sheet.write(0, i, field_info['header'])
# #         
# #         # Data
# #         for i, record in enumerate(self.all_records()):
# #             for j, field_name in enumerate(self.__schema.list_field_names()):
# #                 if i < 65530: # Limit for Excel
# #                     try:
# #                         value = record.values[field_name]
# #                         schema = self.__schema
# #                         value = schema.format_field_value_for_excel(field_name,
# #                                                                     value)
# #                         data_sheet.write(1+i, j, value)
# #                     except KeyError:
# #                         data_sheet.write(1+i, j, '')
# #         
# #         # Schema Description
# #         schema_sheet.write(0, 0, 'Field')
# #         schema_sheet.write(0, 1, 'ID')
# #         schema_sheet.write(0, 2, 'Description')
# #         schema_sheet.write(0, 3, 'Type')
# #         for i, field_info in enumerate(self.schema.list_fields()):
# #             schema_sheet.write(i+1, 0, field_info['header'])
# #             schema_sheet.write(i+1, 1, field_info['name'])
# #             schema_sheet.write(i+1, 2, field_info['desc'])
# #             schema_sheet.write(i+1, 3, field_info['type'])
# #         
# #         # Save File
# #         book.save(path)
#         
#         
#     def export_as_csv(self, path):
#         schema = self.__schema
#         format_csv = schema.format_field_value_for_csv
#         
#         # Init File
#         with open(path, 'wt') as fh:
#             writer = csv.writer(fh)
#         
#             # Data Header
#             writer.writerow([f['header'] for f in self.schema.list_fields()])
#             
#             # Data
#             for record in self.all_records():
#                 values = list()
#                 for field_name in self.__schema.list_field_names():
#                     try:
#                         value = record.values[field_name]
#                         value = format_csv(field_name, value)
#                     except KeyError:
#                         value = ''
#                     values.append(value)
#                 writer.writerow(values)
                