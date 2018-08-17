
from .TraceEvent import TraceEvent

class EtlState:
    '''Track ETL state in TraceData'''

    INIT_STATE = 'init'
    RUNNING_STATE = 'running'
    FINISHED_STATE = 'finished'
    ERROR_STATE = 'error'

    def __init__(self):
        self.state = self.INIT_STATE


class TraceEtlState(TraceEvent):

    def _list_required_keys(self):
        return (
            'state',    # State code of the ETL
        )

    def apply_to_trace_data(self, trace_data):
        trace_data.etl_state.state = self.state
