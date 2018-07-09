'''
Created on Apr 16, 2014

@author: nshearer
'''
import tkinter as tk

from netl.EtlProcessor import EtlProcessor, EtlProcessorDataPort

class PrcStatusGridComponent(object):
    
    def __init__(self, parent_for_widgets):
        self.__wp = parent_for_widgets
    
    
    def _build_label(self, str_var, anchor=tk.NW, width=None):
        '''Create a StringVar and a TK Widget to display info about prc'''
        label = tk.Label(
            self.__wp,
            textvariable=str_var,
            background="#ffffff",
            anchor=anchor)
        if width is not None:
            label.config(width=width)
            
        return label
    
    
    def _sv(self, init_text=None):
        '''Build a string variable'''
        var = tk.StringVar()
        if init_text is not None:
            var.set(init_text)
        return var
    

# -- DataPort Level (multiple per process) ------------------------------------

class DataPortInGrid(PrcStatusGridComponent):
    '''Holds the TK elements used to describe the status of a data port'''

    def __init__(self, parent_for_widgets, processor, direction, dataport):
        super(DataPortInGrid, self).__init__(parent_for_widgets)
        self.prc = processor
        self.port = dataport
        self.direction = direction
        
        assert(direction in ['input', 'output'])
        assert(isinstance(processor, EtlProcessor))
        assert(isinstance(dataport, EtlProcessorDataPort))

        self.direction_abbrv = 'I'
        if self.direction == 'output':
            self.direction_abbrv = 'O'

        formatted_name = "%s: %s" % (self.direction_abbrv, dataport.name)
        self.name_var = self._sv(formatted_name)
        self.name_label = self._build_label(self.name_var)
        
        self.schema_var = self._sv(dataport.schema.__class__.__name__)
        self.schema_label = self._build_label(self.schema_var)
        
        self.buffered_var = self._sv('--')
        self.buffered_label = self._build_label(self.buffered_var,
                                                anchor=tk.E)
        
        self.count_var = self._sv('--')
        self.count_label = self._build_label(self.count_var,
                                             anchor=tk.E)
        

                    
# -- Root Level: Processes in the grid ----------------------------------------
    
class ProcessInGrid(PrcStatusGridComponent):
    '''Holds the TK elements used to describe the status of a processor'''
    
    def __init__(self, parent_for_widgets, prc_name, processor):
        super(ProcessInGrid, self).__init__(parent_for_widgets)
        self.prc = processor
        
        assert(isinstance(processor, EtlProcessor))
        
        self.prc_var = self._sv(prc_name)
        self.prc_label = self._build_label(self.prc_var, width=20)
        
        self.prc_class_var = self._sv(self.prc.__class__.__name__)
        self.prc_class_label = self._build_label(self.prc_class_var)
        
        self.prc_status_var = self._sv("Pending")
        self.prc_status_label = self._build_label(self.prc_status_var)
        
        self.inputs = dict()
        if self.prc.list_inputs() is not None:
            for port in self.prc.list_inputs():
                port_widgets = DataPortInGrid(parent_for_widgets,  processor, 
                                              'input',  port)
                self.inputs[port.name] = port_widgets
        
        self.outputs = dict()
        if self.prc.list_outputs() is not None:
            for port in self.prc.list_outputs():
                port_widgets = DataPortInGrid(parent_for_widgets,  processor, 
                                              'output',  port)
                self.outputs[port.name] = port_widgets
            
            

        
    
        
        