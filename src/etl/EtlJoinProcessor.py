'''
Created on Dec 28, 2012

@author: nshearer
'''
import mydevtools

from EtlProcessor import EtlProcessor


class EtlJoinProcessor(EtlProcessor):
    '''Join one set of records to another'''
    
    def __init__(self):
        super(EtlJoinProcessor, self).__init__()
        self.__match_keys = dict()      # Match Key -> (input_set, Record Key)
        self.__lookup_inputs_processed = False
        
        
    def list_inputs(self):
        for p_input in self.list_lookup_inputs():
            yield p_input
        for p_input in self.list_subject_inputs():
            yield p_input
        
    # -- Override these -------------------------------------------------------
        
    def list_lookup_inputs(self):
        '''List inputs that contain the records to ref against
        
        These record sets must be indexed
        '''
        mydevtools.abstract_method(self, 'list_lookup_inputs')
        
        
    def list_subject_inputs(self):
        '''List inputs that contain the records to find refs for'''
        mydevtools.abstract_method(self, 'list_subject_inputs')
        
        
    def build_lookup_record_key(self, lookup_record):
        '''Build a key to be used for matching subject records to'''
        mydevtools.abstract_method(self, 'build_lookup_record_key')
        
        
    def build_lookup_key(self, record):
        '''Build a key to use to find a lookup record'''
        mydevtools.abstract_method(self, 'build_lookup_key')
        
        
    # -- Common join logic ----------------------------------------------------
        
    def gen_output(self, name, inputs, record_set):
        '''Generate named output data.
        
        Dynamically calls 'gen_<name>_output' method
        
        @param name: Name of the output to generate
        @param inputs: Dictionary of connected input datasets
        @param record_set: Container to populate with records
        '''
        if not self.__lookup_inputs_processed:
            # Generate keys for lookup records
            for data_port in self.list_lookup_inputs():
                for input_set in inputs[data_port.name]:
                    for record in input_set.all_records():
                        
                        # Build a Match key for this lookup record
                        match_key = self.build_lookup_record_key(record)
                        if match_key is None:
                            msg = "Did not build a match key for this record"
                            msg = record.create_msg(msg)
                            raise Exception(msg)
                            
                        # Determine associated index
                        rec_index = record.index
                        if rec_index is None:
                            msg = "Record in lookup input has no index."
                            msg = record.create_msg(msg)
                            raise Exception(msg)
                        
                        # Make sure match key is unique
                        if self.__match_keys.has_key(match_key):
                            handle = self._handle_duplicate_lookup_match_key
                            handle(match_key, record)
                        
                        # Store
                        else:
                            store_rec = self._store_lookup_record
                            store_rec(match_key, input_set, rec_index)
            self.__lookup_inputs_processed = True
                    
        # Call Parent to process subject records
        super(EtlJoinProcessor, self).gen_output(name, inputs, record_set)
        
        
    #def gen_invoices_output(self, inputs, output_set):
    #        for record_set in inputs['invoices']:
    #            for record in record_set.all_records():
    #                ref_record = self.lookup(record)
    #                if ref_record is not None:
    #                    # Get values from subject
    #                    values = record.values
    #                    
    #                    # Copy in values from lookup record
    #                    for name in ['pidm', 'name', 'ssn']:
    #                        values[name] = ref_record[name]
    #                
    #                    # Output record
    #                    output_set.add_record(values)
        
        
        
    def lookup(self, record):
        '''Find record in lookup sets for this record'''
        # Build a Match key for this lookup record
        match_key = self.build_lookup_key(record)
        if match_key is None:
            msg = "Did not build a match key for this record"
            msg = record.create_msg(msg)
            raise Exception(msg)
        
        # Find match
        if self.__match_keys.has_key(match_key):
            input_set, lookup_index = self.__match_keys[match_key]
            return input_set.get_record(lookup_index)
            
        return None
            
            
    def _handle_duplicate_lookup_match_key(self, match_key, record):
            msg = "Duplicated match key '%s'" % (match_key)
            msg = record.create_msg(msg)
            raise Exception(msg)

            
    def _store_lookup_record(self, match_key, lookup_set, index):
        self.__match_keys[match_key] = (lookup_set, index)