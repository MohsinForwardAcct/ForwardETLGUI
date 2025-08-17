
from flet import *
import SYSConnectToServers as CS
import GUIForm as GF


class GUIVariableList(Container):
    
    def __init__(self, page, download):
        super().__init__()

        self.page = page
        self.Download = download
        self.ETL = CS.ConnectToODSServer()
        self.ColumnNames = []; self.RowsData = []; self.RowValues = ''
        self.ColumnSelect = 'VariableList'; self.ReportTitle = 'Variable List'
        self.Query = f"select VariableNameID, ColumnName, DataType, Source, TableName, Description from RESVVariablesLogic"
        self.GetScreenChange()
        self.ReportingScreen

    # handle screen change for variable logic

    def GetScreenChange(self):
        self.ReportingScreen = GF.FormScreen(self.page, self.Download, self.Query, self.ColumnSelect, self.ReportTitle).GetFormScreen()




