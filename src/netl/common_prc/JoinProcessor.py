from abc import ABCMeta, abstractmethod

from ..EtlComponent import EtlComponent


class JoinComponent(EtlComponent):
    '''Join each record on the right to one on the left
    
                       +----------------+                                     
                       |                +--->  joined                         
            left   +--->                |                                     
                       |  JoinProcssor  +--->  left_not_joined                
            right  +--->                |                                     
                       |                +--->  right_not_joined               
                       +----------------+

    The three outputs are:
    
      joined: Contains each of the right records joined to a single matching left
        record if one was found to join to.
            
      left_not_joined: Contains each of the left records that was not matched to
        any record on the right input
                     
      right_not_joined: Contains each of the right records that was not matched to
        any record on the left input

    Joining is done by indexing all records on the left by the join key, and
    then processing right records and joining them.  If you have more records
    expected on one input than the other, put the input with fewer records into
    left.

    '''
    __metacass__ = ABCMeta
    
    def __init__(self, name):
        super(JoinProcessor, self).__init__(name)
        self.__shelf = RecordShelf()
        
        self.df_create_input_port('left')
        self.df_create_input_port('right')
        
        self.df_create_output_port('joined')
        self.df_create_output_port('left_not_joined')
        self.df_create_output_port('right_not_joined')
        
        
        
    # -- Override these -------------------------------------------------------
        
    @abstractmethod
    def calc_left_join_key(self, left_record):
        '''Return a string to use for matching to records on rith input'''

    @abstractmethod
    def calc_right_join_key(self, left_record):
        '''Return a string to use for matching to records on rith input'''
        
    @abstractmethod
    def join_left_to_right(self, right_record, left_record):
        '''Return a new record with the information joined'''
        
        
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
                        if match_key in self.__match_keys:
                            handle = self._handle_duplicate_lookup_match_key
                            handle(match_key, record)
                        
                        # Store
                        else:
                            store_rec = self._store_lookup_record
                            store_rec(match_key, input_set, rec_index)
            self.__lookup_inputs_processed = True
                    
        # Call Parent to process subject records
        super(JoinProcessor, self).gen_output(name, inputs, record_set)
        
        
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
        if match_key in self.__match_keys:
            input_set, lookup_index = self.__match_keys[match_key]
            return input_set.get_record(lookup_index)
            
        return None
            
            
    def _handle_duplicate_lookup_match_key(self, match_key, record):
            msg = "Duplicated match key '%s'" % (match_key)
            msg = record.create_msg(msg)
            raise Exception(msg)

            
    def _store_lookup_record(self, match_key, lookup_set, index):
        self.__match_keys[match_key] = (lookup_set, index)