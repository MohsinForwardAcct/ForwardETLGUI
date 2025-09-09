
from flet import *
import SYSConnectToServers as CS
import pandas as pd
import GUIBrdxReports as BrdxReport
import GUIBrdxTemplates as BrdxTemplate
import GUIBrdxVariables as BrdxVariables

class Bordereaux(Container):

    def __init__(self, page, download, username):
        super().__init__()

        self.page = page
        self.Download = download
        self.UserName = username
        self.ETL = CS.ConnectToODSServer()
        self.ColumnNames = []; self.RowsData = []; self.RowValues = ''
        self.ColumnSelect = 'Bordereaux'; self.ReportTitle = 'Bordereaux'
        self.Query = f"select distinct ReportingYear from FACTData"
        self.TableData = pd.DataFrame({'Bordereaux':['Bordereaux Reports','Bordereaux Templates','Bordereaux Variables']})
        self.GetReportScreen()
        self.ReportingScreen = Column(expand=True, controls=[self.ReportScreen])

    # screen handler   

    def GetReportScreen(self):
        self.GetTableData()
        self.ReportTableData = DataTable(expand=True,border_radius=8,border=border.all(2,"#263238"),columns=self.ColumnNames,rows=self.RowsData)
        self.ReportHeader = Container(expand=True, height=50, bgcolor="#AD1457",border_radius=border_radius.only(top_left=15,top_right=15),
                            padding=padding.only(left=15,right=15),content=Row(expand=True,alignment=MainAxisAlignment.SPACE_BETWEEN,
                            controls=[
                            Container(expand=True,content=Text(self.ReportTitle,text_align="Center",size=18,color="white",weight='bold'))]))
        self.ReportScreen = Column(expand=True,controls=[Row(controls=[Row(expand=True,controls=[self.ReportHeader]),Divider(height=2,
                            color="transparent")]),Column(scroll="hidden",expand=True,controls=[Row([self.ReportTableData])])])
        
    # get table data
       
    def GetTableData(self):
        for cols in self.TableData.columns.values: self.ColumnNames.append(DataColumn(Text(cols,size=15,color='#AD1457',weight='bold')))
        for index, rows in self.TableData.iterrows():
            self.RowsData.append(DataRow(cells=[DataCell(Text(rows[self.ColumnSelect],color="white",size=15,width=700))],
            on_select_changed = self.GetNavigateFront))

    # screen updates handler

    async def UpdateScreen(self, MainScreen):
        self.ReportingScreen.controls.clear()
        self.ReportingScreen.controls.append(MainScreen)
        await self.ReportingScreen.update_async()

    # screen changes handler

    async def GetScreenChange(self):
        self.ColumnNames.clear(); self.RowsData.clear()
        self.GetReportScreen()
        self.ReportingScreen.controls.clear()
        self.ReportingScreen.controls.append(self.ReportScreen)
        await self.ReportingScreen.update_async()

    # move forward handler

    async def GetNavigateFront(self, e):
        LabelName = e.control.cells[0].content.value
        if LabelName == "Bordereaux Reports": MainScreen = BrdxReport.BrdxReports(self.page, self.Download, self.UserName).ReportingScreen; await self.UpdateScreen(MainScreen)
        elif LabelName == "Bordereaux Templates": MainScreen = BrdxTemplate.BrdxTemplates(self.page, self.Download, self.UserName).ReportingScreen; await self.UpdateScreen(MainScreen)
        elif LabelName == "Bordereaux Variables": MainScreen = BrdxVariables.BrdxVariables(self.page, self.Download, self.UserName).ReportingScreen; await self.UpdateScreen(MainScreen)
        print(LabelName)

