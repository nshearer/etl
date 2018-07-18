from bunch import Bunch

from .TraceData import TraceData

class EnvelopeTrace(TraceData):
    '''A transmission trace for a record in the ETL'''

    # @staticmethod
    # TODO: get_connection_stats(trace_db):
    #     '''Get some stats on messages sent on connections'''
    #     for row in trace_db.execute_select("select * from v_connection_stats"):
    #         yield ConnectionStats(**row)



class TraceRecordDispatch(TraceData):
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

