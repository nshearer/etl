'''
Created on Dec 27, 2012

@author: nshearer
'''
import mydevtools

class EtlProcessorDataPort(object):
    '''Specify a name for input or output record sets'''
    def __init__(self, name, schema):
        self.name = name
        self.schema = schema


class EtlProcessor(object):
    '''Takes 0 or more inputs and generates 0 or more outputs'''
    
    def __init__(self):
        self.data_dir_path = None
        self.tmp_dir_path = None
    
    
    def list_inputs(self):
        mydevtools.abstract_method(self, 'list_inputs')
    
    
    def list_outputs(self):
        mydevtools.abstract_method(self, 'list_outputs')
        
        
    def gen_output(self, name, inputs, record_set):
        '''Generate named output data.
        
        Dynamically calls 'gen_<name>_output' method
        
        @param name: Name of the output to generate
        @param inputs: Dictionary of connected input datasets
        @param record_set: Container to populate with records
        '''
        # Get method to generate records
        gen_name = 'gen_%s_output' % (name)
        try:
            gen = getattr(self, gen_name)
        except AttributeError:
            mydevtools.abstract_method(self, gen_name)
        
        # Call method to generate records
        gen(inputs, record_set)
        
    #def gen_<name>_output(self, inputs, output_set):
    #    for record_set in inputs['main_input_name']:
    #        for record in record_set.all_records():
    #            output_set.add_record({
    #                'Field':   record['field_value'],
    #                }, index=field_value)
    
        