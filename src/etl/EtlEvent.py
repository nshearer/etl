from abc import ABCMeta

class EtlEvent(object):
    '''Notify an EtlProcessor of an event'''
    __metaclass__ = ABCMeta
    def __init__(self, event_type):
        self.__type = event_type
    @property
    def type(self):
        return self.__type
    
    
class InputRecordRecieved(EtlEvent):
    '''An input record was received'''
    def __init__(self, input_name, conn_id):
        super(InputRecordRecieved, self).__init__('input_record')
        self.input_name = input_name
        self.conn_id = conn_id
    # Note: Wanted to record source processor here, but events may arrive
    #       at EtlProcessor.event_loop() in a different order than the
    #       input record queue due to threading.
    
    
# class PrcConnectedEvent(EtlEvent):
#     '''Inform a processor that another processor has connected to it's input'''
#     def __init__(self, input_name, src_prc_name, src_output_name, conn_id):
#         super(PrcConnectedEvent, self).__init__('input_connected')
#         self.input_name = input_name
#         self.src_prc_name = src_prc_name
#         self.src_output_name = src_output_name
#         self.conn_id = conn_id
#         
#         
class PrcDisconnectedEvent(EtlEvent):
    '''A processor that a supplying processor has disconnected from it's input'''
    def __init__(self, input_name, src_prc_name, src_output_name, conn_id):
        super(PrcDisconnectedEvent, self).__init__('input_disconnected')
        self.input_name = input_name
        self.src_prc_name = src_prc_name
        self.src_output_name = src_output_name
        self.conn_id = conn_id
        
        
# class AllPrcDisconnectedEvent(EtlEvent):
#     '''All processors connected to a given input have clossed
#     
#     This will be raised once per Workflow life-cycle to indicate to the
#     processor that no more records will be received on the specified input
#     '''
#     def __init__(self, input_name):
#         super(AllPrcDisconnectedEvent, self).__init__('all_input_disconnected')
#         self.input_name = input_name

                
# class EtlAbort(EtlEvent):
#     '''Abort the execution of the Etl Engine due to a failed processor'''
#     def __init__(self, error_msg):
#         super(InputRecordRecieved, self).__init__('abort_etl')
#         self.error_msg = error_msg
    
