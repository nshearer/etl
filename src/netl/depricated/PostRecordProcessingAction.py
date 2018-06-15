from abc import ABCMeta
 
 
class PostRecordProcessingAction(object, metaclass=ABCMeta):
    '''Action to take after a record is processed by prc_input_record()
     
    This instructs the Processors Event on what to do with the input record
    that was just processed.  This is provided primarily to allow a processor
    to declare that it did not process an input record, and will request it
    later
    '''
    def __init__(self, action_code):
        self.__code = action_code
    @property
    def code(self):
        return self.__code
     
     
class RecordConsumed(PostRecordProcessingAction):
    '''Signify that the record was processed, and can be discarded.
     
    This is the default assumed action if None is returned from
    prc_input_record()
    '''
    def __init__(self):
        super(RecordConsumed, self).__init__('record_consumed')
         
         
class HoldRecord(PostRecordProcessingAction):
    '''Instruct processor that record has not been processed yet.
     
    This holds the record on the input queue until GetNextInputRecord is
    called
    '''
    def __init__(self):
        super(HoldRecord, self).__init__('hold_record')
 
 
class GetNextInputRecord(PostRecordProcessingAction):
    '''Instruct processor to immediately retrieve the next buffered record
     
    This grabs the next record on the specified input and calls
    prc_input_record() again without processing additional events on the event
    queue first.  This is typically used to process through previously held
    (HoldRecord) records.  If no additional records exist on the specified
    input, then processor will continue processing the event queue.
     
    This response implies RecordConsumed, and the record passed in to
    prc_input_record() will be popped off the incoming queue.
    '''
    def __init__(self, input_name):
        super(HoldRecord, self).__init__('get_next_record')
        self.input_name = input_name
                 