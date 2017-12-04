import Tkinter as tk

from TkProcessorStatusGrid import TkProcessorStatusGrid

class TkWorkflowMonitor(object):
    '''GUI Application to run, monitor, and trace a workflow'''

    def __init__(self, workflow):
        '''Init
        
        @param workflow: netl.Workflow object with ETL components added/connected
        '''
        
        wf = self.__workflow = workflow
        
        # -- Init tkinter Main Window -----------------------------------------
        
        self.__root = tk.Tk()
        self.__root.title("%s ETL Workflow" % (wf.__class__.__name__))
        
        # -- Build Processor Status Grid --------------------------------------
        
        self.prc_status_grid = TkProcessorStatusGrid(self.__root, wf)
#         self.prc_status_grid.grid(row=0, column=0)
        
        # -- Add Control Buttons ----------------------------------------------
        
#         control_frame = tk.Frame(self.__root)
#         control_frame.grid(row=1, column=0)
        
#         self.execute_btn = tk.Button(control_frame, text="Start")
        
#         frame = Frame(root)
#         frame.pack()
#         
#         bottomframe = Frame(root)
#         bottomframe.pack( side = BOTTOM )
#         
#         redbutton = Button(frame, text="Red", fg="red")
#         redbutton.pack( side = LEFT)
#         
#         greenbutton = Button(frame, text="Brown", fg="brown")
#         greenbutton.pack( side = LEFT )
#         
#         bluebutton = Button(frame, text="Blue", fg="blue")
#         bluebutton.pack( side = LEFT )
#         
#         blackbutton = Button(bottomframe, text="Black", fg="black")
#         blackbutton.pack( side = BOTTOM)
        
    
    def run_gui(self):
        self.__root.mainloop()
        
    
