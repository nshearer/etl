
class EtlBuildError(Exception):
    '''Error with the structure of the ETL graph'''
    
    def __init__(self, prc_name, prc_class_name, error_msg, possible_values):
        '''Init
        
        @param prc_name: Name of the processor in the ETL graph
        @param prc_class_name: Processor class name
        @param error_msg: Description of the error encountered
        @param possible_values: List of correct values that could have been used
        '''
        
        self.prc_name = prc_name
        self.prc_class_name = prc_class_name
        self.etl_build_error_msg = error_msg
        self.possible_values = possible_values
        
        # Limit possible_values to 6
        if possible_values is list and len(possible_values) > 6:
            possible_values = possible_values[:6]
            possible_values.append('...')
            
        # Format message for base exception
        msg = ""
        if prc_name is not None or prc_class_name is not None:
            if prc_name is None:
                prc_name = "[None]"
            if prc_class_name is None:
                prc_class_name = "[None]"
            msg = "Error with processor %s (%s): " % (prc_name, prc_class_name)
        else:
            msg = "Error with ETL Workflow: "
        
        super(EtlBuildError, self).__init__(msg + error_msg)
        
         
    def get_prc_desc(self):
        if self.prc_name is not None or self.prc_class_name is not None:
            return "%s (%s)" % (self.prc_name, self.prc_class_name)
        return None

    
    def print_std_error(self):
        print "ERROR: ETL Build Error Occurred"
        print "  Message: " + self.etl_build_error_msg 
        if self.get_prc_desc() is not None:
            print "  Processor: " + self.get_prc_desc()
        if self.possible_values is not None:
            possible_values = [str(v) for v in self.possible_values]
            print "  Possible Values: " + ", ".join(possible_values)
            
            