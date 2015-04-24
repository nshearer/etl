
class WorkflowDataPath(object):
    '''Describes a connection between EtlProcessors'''
    
    def __init__(self):
        self.src_prc_name = None
        self.src_prc = None
        self.output_name = None
        self.output_schema = None
        self.dst_prc_name = None
        self.input_name = None
        self.input_schema = None
        self.dst_prc = None
        
        # -- Used by EtlProcessorEventManager --
        self.dst_prc_manager = None

