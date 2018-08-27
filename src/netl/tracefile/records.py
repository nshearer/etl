from bunch import Bunch

from .TraceEvent import TraceEvent

# class EnvelopeTrace(TraceEvent):
#     '''A transmission trace for a record in the ETL'''
#
#     # @staticmethod
#     # TODO: get_connection_stats(trace_db):
#     #     '''Get some stats on messages sent on connections'''
#     #     for row in trace_db.execute_select("select * from v_connection_stats"):
#     #         yield ConnectionStats(**row)



class TraceRecordDispatch(TraceEvent):
    '''Record that a record was sent out'''

    def _list_required_keys(self):
        return (
            'from_comp_name',   #
            'from_comp_id',     #
            'from_port_id',     #
            'from_port_name',   #
            'record_id',        #
            'to_comp_name',     #
            'to_comp_id',       #
            'to_port_name',     #
            'to_port_id',       #
            'to_comp_name',     #
            'to_comp_id',       #
            'to_port_name',     #
            'to_port_id',       #
        )


    def apply_to_trace_data(self, trace_data):
        '''
        Apply this trace event to the TraceData class

        Assume write lock is already obtained

        :param trace_data: TraceData collecting all trace data from trace file
        '''
        print("TODO: TraceRecordDispatch.apply_to_trace_data()")


class TraceRecord(TraceEvent):
    '''A record in the ETL'''

    def _list_required_keys(self):
        return (
            'type',     # Record type code
            'serial',   # Unique ID of the record
            'attrs',    # repr of the record values
        )

    def apply_to_trace_data(self, trace_data):
        '''
        Apply this trace event to the TraceData class

        Assume write lock is already obtained

        :param trace_data: TraceData collecting all trace data from trace file
        '''
        print("TODO: TraceRecord.apply_to_trace_data()")



