import Tkinter as tk

class TkScrollableGrid(tk.Frame):
    '''Create a scrollable grid / table of data
    
    This implementation is intended for a static table (no new columns or rows
    after .build_grid())
    
    Call these methods to build out the table:
      .add_column(name, header)
      .add_row() -> row_num
      .set_value(row_num, col_name, tk_widget)
      
    Then call .pack()
    '''
    
    def __init__(self, parent):
        tk.Frame.__init__(self, parent)
        
        self.__parent = parent
        self.__columns = list() # {'name': string, 'header': string, 'width': }
        self.__col_posn = dict()
        self.__grid_built = False
        self.__rows = list() # [widget_1, widget_2, ...]
    
    
        # Init Canvas and make scrollable
        self.canvas = tk.Canvas(self.__parent, borderwidth=1)
        self.frame = tk.Frame(self.canvas, background="#ffffff")
        self.vsb = tk.Scrollbar(self.__parent, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.vsb.set)

        self.vsb.pack(side="right", fill="y")
        self.canvas.pack(side="left", fill="both", expand=True)
        self.canvas.create_window((4,4), window=self.frame, anchor="nw", 
                                  tags="self.frame")

        self.frame.bind("<Configure>", self.OnFrameConfigure)
        
    
    
    def add_column(self, name, header=None, width=None):
        '''Add a column to the table.
        
        @param name: Name to reference the column
        @param header: If set, will create a header row to the table
        '''
        if self.__grid_built:
            raise Exception("Grid already built")
        
        self.__columns.append({'name': name,
                               'header': header,
                               'width': width})
        self.__col_posn[name] = len(self.__columns) - 1
        
        
    def get_col_name_by_pos(self, pos):
        return self.__columns[pos]['name']


    def add_row(self):
        if self.__grid_built:
            raise Exception("Grid already built")
        
        self.__rows.append(list())
        return len(self.__rows) - 1
    
    
    def set_value(self, row_num, col_name, tk_widget):
        col_num = self.__col_posn[col_name]
        
        while len(self.__rows[row_num]) < col_num + 1:
            self.__rows[row_num].append(None)
            
        self.__rows[row_num][col_num] = tk_widget
        
        
    def build_grid(self):
        if self.__grid_built:
            raise Exception("Grid already built")
        self.__grid_built = True

        # Populate Grid: Header
        grid_row = 0
        if self.has_header():
            for col_num, col in enumerate(self.__columns):
                if col['header'] is not None:
                    if col['width'] is None:
                        tk.Label(self.parent_for_value,
                                 text=col['header'],
                                 borderwidth="1",
                                 padx=3, pady=2,
                                 relief="solid",
                                 background="#ffffff").grid(row=grid_row,
                                                            column=col_num,
                                                            sticky=tk.W + tk.E)
                    else:
                        tk.Label(self.parent_for_value,
                                 text=col['header'],
                                 borderwidth="1", 
                                 padx=3, pady=2,
                                 relief="solid",
                                 width=col['width'],
                                 background="#ffffff").grid(row=grid_row,
                                                            column=col_num,
                                                            sticky=tk.W + tk.E)
                        
            grid_row += 1
        
        # Populate Grid: Data
        for row in self.__rows:
            for col_num, value in enumerate(row):
                if value.__class__ is str:
                    value = tk.Label(
                        self.parent_for_value,
                        text=value,
                        background="#ffffff",
                        anchor=tk.NW)
                if value is not None:
                    value.grid(row=grid_row, column=col_num, sticky=tk.W + tk.E)
            grid_row += 1
            
        self.frame.pack(fill=tk.BOTH)
                    
                     
    def has_header(self):
        for col in self.__columns:
            if col['header'] is not None:
                return True
        return False 


    @property
    def parent_for_value(self):
        '''Use this if constructing your own widgets to place in the grid'''
        return self.frame
    
    
    def OnFrameConfigure(self, event):
        '''Reset the scroll region to encompass the inner frame'''
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        
        