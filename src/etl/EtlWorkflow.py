'''
Created on Dec 27, 2012

@author: nshearer
'''
import os

from EtlProcessorBase import EtlProcessorBase

class EtlWorkflow(EtlProcessorBase):
    '''Encapsulates an ETL workflow
    
    This class is used to organize all of the components in the workflow.
    It is itself a processor in the ETL Workflow, refered to as the root
    processor.  Unlike child processors based on EtlProcessor, though,
    this root processor runs in the same thread as the invoking code.
    
    Definition of ETL:
    ------------------
    from [Wikipedia](http://en.wikipedia.org/wiki/Extract,_transform,_load)

    In computing, Extract, Transform and Load (ETL) refers to a process in
    database usage and especially in data warehousing that:

    Extracts data from homogeneous or heterogeneous data sources Transforms the
    data for storing it in proper format or structure for querying and analysis
    purpose Loads it into the final target (database, more specifically,
    operational data store, data mart, or data warehouse) Usually all the three
    phases execute in parallel since the data extraction takes time, so while
    the data is being pulled another transformation process executes, processing
    the already received data and prepares the data for loading and as soon as
    there is some data ready to be loaded into the target, the data loading
    kicks off without waiting for the completion of the previous phases.

    ETL systems commonly integrate data from multiple applications(systems),
    typically developed and supported by different vendors or hosted on separate
    computer hardware. The disparate systems containing the original data are
    frequently managed and operated by different employees. For example a cost
    accounting system may combine data from payroll, sales and purchasing.


    Creating an ETL Process
    -----------------------

    In order to define an ETL process, the developer is encouraged to
    subclass this class and then define and connect the processors.
    This class does not need to be subclassed to create an ETL process,
    though, as you can just instantiate it and call the methods.
    
     1) Define your processors by subclassing EtlProcessor, or using the
        common processors under etl.common

     2) Call add_processor() to add your processors to the Workflow

     3) Call connect() to connect the output ports of processors to the
        input ports of other processors

     4) Call assign_processor_output() to connect the output ports of
        processors to an input port of this Workflow object.  This allows
        you to define a path for records to exit the ETL workflow and
        be returned to the calling code.  When you call the workflow.
        execute() method to run the ETL process, and records dispatched
        on the specified port will be yielded back to the calling function.

      5) Call exectue() - Run the workflow to generate the desired output. 
    '''
    
    def __init__(self):
        super(EtlWorkflow, self).__init__(name='main')

        self.default_data_directory = os.curdir # was data_dir_path

        self.temp_directory = os.path.join(self.default_data_directory, 'tmp')
        # was tmp_dir_path
        
        
        
    # -- Public Methods -------------------------------------------------------
        
    def add_processor(self, prc):
        '''Add a processor to the workflow
        
        @param prc: EtlProcessor class
        '''
        self.df_add_child_processor(prc)
        
        
    def connect(self, source_prc, source_port, dst_prc, dst_port):
        '''Connect the output of one child processor to the input of another
        
        @param source_prc: Name of the source child processor
        @param source_port: Name of the output port on the source processor
        @param dst_prc: Name of the destination child processor
        @param dst_port: Name of the input port on the destination processor
        '''
        self.df_connect_children(source_prc, source_port, dst_prc, dst_port)
        
        
        
    def execute(self):
        '''Execute this workflow
        
        Yields records received
        '''
        for yield_record in self.run_processor():
            yield yield_record
        
    
    
    def save_records(self, prc_name, output_name, filename):
        '''Output records to a file from a processor for review'''
        raise NotImplementedError("TODO")

        # 1) Connect port output to this processor
        # 2) Open file for output
        # 3) Write output as rows are received
        # 4) Close file when Workflow is done

        # Old Code:
        #
        # path = os.path.join(self.default_data_directory, 'reports', filename) + '.xls'
        # data = self.get_output(prc_name, output_name)
        #
        # # Inform User
        # msg = "Saving '%s' output from '%s' to %s"
        # print msg % (output_name, prc_name, path)
        #
        # # Export
        # data.export_as_excel(path)
        #
        # data.export_as_csv(os.path.join(self.default_data_directory, 'reports', filename) + '.csv')
        
        
    # def get_output(self, prc_name, output_name):
    #     '''Get the output of a processor (running the processor if needed)'''
        
    #     # Generate record set if not cached
    #     if not self.__record_sets[prc_name].has_key(output_name):
            
    #         prc = self.__processors[prc_name]
            
    #         # Get required inputs
    #         inputs = dict()
    #         for p_input in prc.list_inputs():
    #             inputs[p_input.name] = list()
    #             for conn in self.__connections[prc_name][p_input.name]:
    #                 input_records = self.get_output(conn.src_prc_name,
    #                                                 conn.output_name)
    #                 inputs[p_input.name].append(input_records)
                    
    #         # Init RecordSet to contain output
    #         output_schema = self._get_output_schema(prc_name, output_name)
    #         out_records = EtlRecordSet(prc_name, output_name, output_schema)
            
    #         # Prepare processor
    #         prc.default_data_directory = self.default_data_directory
    #         prc.temp_directory = self.temp_directory
            
    #         # Inform User
    #         msg = "Running processor '%s' to generate '%s'"
    #         print msg % (prc_name, output_name)
            
    #         # Generate output
    #         prc.gen_output(output_name, inputs, out_records)
            
    #         # Cache output
    #         self.__record_sets[prc_name][output_name] = out_records
            
    #     # Returned cached output
    #     return self.__record_sets[prc_name][output_name]
        
        
    # -- Utility Methods ------------------------------------------------------
        
    # def _get_prc_output_info(self, prc_name, output_name):
    #     '''Get EtlProcessorDataPort object from processor for this output'''
    #     if not self.__processors.has_key(prc_name):
    #         raise KeyError("Invalid processor name: %s" % (prc_name))
    #     for output in self.__processors[prc_name].list_outputs():
    #         if output.name == output_name:
    #             return output
    #     return None
    
    
    # def _get_output_schema(self, prc_name, output_name):
    #     '''Get schema from processor for this output'''
    #     info = self._get_prc_output_info(prc_name, output_name)
    #     return info.schema
    
    
    # -- ETL Inspection -------------------------------------------------------
    
    def list_prc_names(self):
        return self.__processors.keys()
    
    
    def get_prc(self, name):
        return self.__processors[name]
    
        
    