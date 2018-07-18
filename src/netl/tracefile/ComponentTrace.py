

from .TraceData import TraceData

class TraceNewComponent(TraceData):
    '''A component in the ETL'''

    INIT_STATE = 'init'
    RUNNING_STATE = 'running'
    FINISHED_STATE = 'finshed'
    ERROR_SATE = 'error'

    def _list_required_keys(self):
        return (
            'component_id',     # Unique, integer ID for this component
            'name',             # Name of the component
            'clsname',          # Class name of the component
            'state',            # Code that represents the state of the component
        )

    # TODO: list_components(trace_db):
    #     results = trace_db.execute_select("select * from components")
    #     for row in results:
    #         yield ComponentTrace(trace_db, **row)
    #
    #
    # TODO: list_ports(self):
    #     return self.db.list_ports_for(self.id)
    #
    # TODO: list_input_ports(self):
    #     return self.db.list_ports_for(self.id, port_type='i')
    #
    # TODO: list_output_ports(self):
    #     return self.db.list_ports_for(self.id, port_type='o')


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


class TraceComponentStateChange(TraceData):

    def _list_required_keys(self):
        return (
            'component_id', # unique, integer ID for this component
            'state',        # New state code

        )
