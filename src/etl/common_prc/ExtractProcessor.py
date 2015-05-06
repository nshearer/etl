
from etl.EtlProcessor import EtlProcessor
from etl.EtlRecord import EtlRecord

class ExtractProcessor(EtlProcessor):
    '''This processor is used to strip out all fields except a few
    
                 +------------------+                             
                 |                  |                             
        in +-----> ExtractProcessor +-----> out               
                 |                  |                             
                 +------------------+                             
    '''
    
    def __init__(self, name, out_schema, out_fields):
        super(ExtractProcessor, self).__init__(name=name)
        self.__schema = out_schema
        self.__fields = out_fields
        
        self.df_create_input_port('in')
        self.df_create_output_port('out')
        
        
    def pr_in_input_record(self, record, from_prc_name, from_port_name):
        
        newrec = EtlRecord(schema=self.__schema)
        newrec.note_src_record(record)
        for fieldname in self.__fields:
            newrec[fieldname] = record[fieldname]
        self.pr_dispatch_output('out', newrec)
