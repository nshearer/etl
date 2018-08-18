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


class TraceRecord(TraceEvent):
    '''A record in the ETL'''

    def _list_required_keys(self):
        return (
            'type',     # Record type code
            'serial',   # Unique ID of the record
            'attrs',    # repr of the record values
        )


