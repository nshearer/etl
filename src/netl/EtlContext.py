

class EtlContext:
    '''
    Contains data about the workflow that should be referenceable by all components

    This should include data that is initialized once before the ETL workflow is started,
    and not changed once execution begins.

    Include:
      ETL Parameters
      Credentials
      Workflow Settings
      Authentication tokens

    Do not include:
      Variables that change as components run
      Shared DB connections
    '''

    def __init__(self):
        self.tmp_dir = None
