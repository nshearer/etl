

from .TraceEvent import TraceEvent


class ComponentState:
    '''Stores the current state of the component'''

    INIT_STATE = 'init'
    RUNNING_STATE = 'running'
    FINISHED_STATE = 'finshed'
    ERROR_SATE = 'error'

    def __init__(self, event):
        self.component_id = event.component_id
        self.name = event.name
        self.clsname = event.clsname
        self.state = event.state
        self.ports = dict()     # [port_id] = PortState()


    STATE_COLORS = {
        INIT_STATE:     '#FFC107', # yellow
        RUNNING_STATE:  '#08B530', # green
        FINISHED_STATE: '#007BFF', # blue
        ERROR_SATE:     '#DC3545', # red
    }
    @property
    def state_color(self):
        try:
            return self.STATE_COLORS[self.state_code]
        except KeyError:
            return '#000000'



class TraceNewComponent(TraceEvent):
    '''A component in the ETL'''

    def _list_required_keys(self):
        return (
            'component_id',     # Unique, integer ID for this component
            'name',             # Name of the component
            'clsname',          # Class name of the component
            'state',            # Code that represents the state of the component
        )

    def apply_to_trace_data(self, trace_data):
        trace_data.components[self.component_id] = ComponentState(self)



class TraceComponentStateChange(TraceEvent):

    def _list_required_keys(self):
        return (
            'component_id', # unique, integer ID for this component
            'state',        # New state code
        )

    def apply_to_trace_data(self, trace_data):
        trace_data.components[self.component_id].state = self.state

