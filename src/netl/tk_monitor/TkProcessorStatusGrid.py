import tkinter as tk

from .TkScrollableGrid import TkScrollableGrid
from .PrcStatusGridComponents import ProcessInGrid, DataPortInGrid

class TkProcessorStatusGrid(TkScrollableGrid):
    '''A scrollable grid showing the status of all processors'''
    
    def __init__(self, parent, workflow):
        self.__parent = parent
        wf = self.__workflow = workflow

        TkScrollableGrid.__init__(self, self.__parent)
        
        # -- Define Columns ---------------------------------------------------
        
         
        self.add_column('prc',         "Processor",        width=20)
        self.add_column('prc_class',   "Class",            )
        self.add_column('status',      "Status",           )
        self.add_column('dataset',     "Data Set",         )
        self.add_column('schema',      "D.S. Schema",      )
        self.add_column('buffered',    "Rec. Buffered",    )
        self.add_column('processed',   "Rec. Processed",   )

        # -- Create components (vars & labels) --------------------------------
        
        self.__components = dict()
        for prc_name in wf.list_prc_names():
            self.__components[prc_name] = ProcessInGrid(self.parent_for_value,
                                                        prc_name,
                                                        wf.get_prc(prc_name))
            
        # -- Layout components on the grid -----------------------------------
        
        for grid_row in self.get_labels_for_grid():
            row_num = self.add_row()
            for i, label in enumerate(grid_row):
                self.set_value(row_num,
                               self.get_col_name_by_pos(i),
                               label)

        
        # -- Layout Grid ------------------------------------------------------
        
        self.build_grid()
        
        
        

    def get_labels_for_grid(self):
        '''Return label widgets to place on grid in format
        
        Keep in mind structure is 1 processor to many dataports
        The current layout is:
            prc_fields    dataport_fields
                          dataport_fields
            prc_fields    dataport_fields
                          dataport_fields
                          dataport_fields
                          
        @return list of fields with None where no label should be placed
        '''
        for prc_name in self.__workflow.list_prc_names():
            prc_comp = self.__components[prc_name]
            comp_port_fields = list(self.get_labels_for_prc_on_grid(prc_comp))
            
            # Init row with processor labels
            row = [prc_comp.prc_label,
                   prc_comp.prc_class_label,
                   prc_comp.prc_status_label,
                   None,
                   None,
                   None,
                   None]
            
            if len(comp_port_fields) == 0:    # Processor has not inputs/outputs
                yield row
            else:
                # Copy port fields into this row
                for port_fields in comp_port_fields:
                    for i in range(len(port_fields)):
                        row[3+i] = port_fields[i]
                
                    yield row
                
                    # Clear out processor fields so we only return them once
                    for i in range(3):
                        row[i] = None
                
                
            
            
    def get_labels_for_prc_on_grid(self, prc_comp):
        '''Return label widgets to place on grid for all dataport for a prc
        
        This returns just the dataport field labels'''
        for name in prc_comp.prc.list_input_names():
            yield (prc_comp.inputs[name].name_label,
                   prc_comp.inputs[name].schema_label,
                   prc_comp.inputs[name].buffered_label,
                   prc_comp.inputs[name].count_label)
        for name in prc_comp.prc.list_output_names():
            yield (prc_comp.outputs[name].name_label,
                   prc_comp.outputs[name].schema_label,
                   prc_comp.outputs[name].buffered_label,
                   prc_comp.outputs[name].count_label)
            
            