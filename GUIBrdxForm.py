
from flet import *
import SYSConnectToServers as CS
import GUIBrdxReports as BrdxReports
import main as mn

class BrdxForm(Container):

    def __init__(self, page, download, username,BrdxReport, MainScreen, ReportTitle):
        super().__init__()

        self.page = page
        self.Download = download
        self.UserName = username
        self.ETL = CS.ConnectToETLServer()
        self.ColumnNames = []; self.RowsData = []; self.RowValues = ''
        self.MainScreen = MainScreen; self.ReportTitle = ReportTitle
        self.TableData = BrdxReport
        self.FormScreen = Column(expand=True, controls=[])
        self.CurrentPage = 1; self.TotalPages = (self.TableData.shape[0] + 10 - 1) // 10

    # create BrdxFormScreen and option buttons 

    async def GetBrdxReportScreen(self):
        for cols in self.TableData.columns.values: self.ColumnNames.append(DataColumn(Text(cols,size=15,color='#AD1457',weight='bold')))
        Start = (self.CurrentPage-1); End = Start + 10
        for index, rows in self.TableData[Start:End].iterrows():
            self.RowsData.append(DataRow(cells=[DataCell(Text(value, color="white",size=13))for value in rows.values]))  
        self.FormTableData = DataTable(expand=True, border=border.all(2,"#ebebeb"), border_radius=10, columns=self.ColumnNames, rows=self.RowsData)
        self.FormFields = Container(expand=True,height=100,bgcolor="white10",border=border.all(1,"#ebebeb"),border_radius=8,padding=15,
                        content=Column(expand=True,controls=[Row(expand=True,controls=self.BrdxScreenHeaderFields())]))
        self.Pagination = Container(content=Row(controls=[self.GetPagination()], alignment=MainAxisAlignment.CENTER))
        self.FormTable =  Container(border_radius=10, col = 8, content=Column(expand=True, scroll='auto', controls=[Row([self.FormTableData])]))
        self.FormHeader = Container(expand=True, height=50, bgcolor="#AD1457",border_radius=border_radius.only(top_left=15,top_right=15),
                        padding=padding.only(left=15,right=15),content=Row(expand=True,alignment=MainAxisAlignment.SPACE_BETWEEN,
                        controls=[IconButton(icon=icons.NAVIGATE_BEFORE_ROUNDED,icon_color=colors.WHITE, on_click = self.GetNavigateBack),
                            Container(expand=True,content=Text(self.ReportTitle,text_align="left",size=18,color="white",weight='bold'))]))  
        self.ReportScreen = Column(expand=True,controls=[Row(expand = False, controls=[self.FormHeader]), Row(expand = False, controls=[self.FormFields]),
                        self.FormTable, 
                        self.Pagination])
        self.FormScreen.controls.append(self.ReportScreen)
        return self.FormScreen
    
    # create brdx form header
    
    def BrdxScreenHeader(self):
        BrdxScreenHeader1 = Row(expand=True,alignment=MainAxisAlignment.SPACE_BETWEEN, controls=[IconButton(icon=icons.NAVIGATE_BEFORE_ROUNDED, 
                icon_color=colors.WHITE, on_click= self.GetNavigateBack), Container(expand=True,content=Text(self.ReportTitle,text_align="Center",
                size=18,color="white",weight='bold'))])
        return BrdxScreenHeader1
    
    # create brdx form fields
    
    def BrdxScreenHeaderFields(self): 
        FeildColumns = ['Total Lines:', 'Pending Lines:', 'Pending Invoice:', 'Resolved %:']; ColumnNames = []; counter = 0
        for cols in FeildColumns: 
            if cols == 'Total Lines:':TextValue= f'{self.TableData.shape[0]}'
            else: TextValue='---'
            ColumnNames.append(Container(expand=2,height=50,bgcolor="#ebebeb",border_radius=6,padding=8,content=Column(spacing=10,controls=[
                Text(value=f'{cols}',size=9,color="black",weight="bold"),TextField(value=TextValue,border_color="transparent",height=10,text_size=18,
                content_padding=0,cursor_color="black",cursor_width=1,cursor_height=16,color="black",disabled=True)])))
            counter = counter + 1
        ColumnNames.append(self.GetFormButton())
        return ColumnNames
    
    # handel screen change events

    async def GetScreenChange(self):
        self.ColumnNames.clear(); self.RowsData.clear()
        await self.GetBrdxReportScreen()
        self.FormScreen.controls.clear()
        self.FormScreen.controls.append(self.ReportScreen)
        await self.FormScreen.update_async()

    # handle back screen events

    async def GetNavigateBack(self, e):
        print(self.MainScreen)
        if self.MainScreen == 'GUIBrdxForm': self.ReportScreen = BrdxReports.BrdxReports(self.page, self.Download, self.UserName).ReportingScreen
        self.FormScreen.controls.clear()
        self.FormScreen.controls.append(self.ReportScreen)
        await self.FormScreen.update_async()

    # create form buttons

    def GetFormButton(self):
        mn.TableData = self.TableData; mn.ReportName = self.ReportTitle
        FormButton = Container(alignment=alignment.center,content= Row(controls=[
            ElevatedButton(on_click=self.Download,bgcolor="#AD1457",color="white",content=Row(controls=[Icon(name=icons.DOWNLOAD_OUTLINED,size=12, color=colors.WHITE),
                    Text("Download", size=10, weight="bold")]),style=ButtonStyle(shape={"":RoundedRectangleBorder(radius=6)},color={"":"white"}),height=35,width=100)]))
        return FormButton

    # handle pagination events
    
    def GetPagination(self):
        PreviousButton = ElevatedButton(on_click=self.GetPreviousPage,bgcolor="#AD1457",color="white",content=Row(controls=[Icon(name=icons.NAVIGATE_BEFORE_OUTLINED,
                size=12, color=colors.WHITE)]),style=ButtonStyle(shape={"":RoundedRectangleBorder(radius=6)},color={"":"white"}),height=20,width=50)
        NextButton = ElevatedButton(on_click=self.GetNextPage,bgcolor="#AD1457",color="white",content=Row(controls=[Icon(name=icons.NAVIGATE_NEXT_OUTLINED,
                size=12, color=colors.WHITE)]),style=ButtonStyle(shape={"":RoundedRectangleBorder(radius=6)},color={"":"white"}),height=20,width=50)
        PageNum = (self.CurrentPage//10) + 1
        PageLabel = Text(f"Page {PageNum} of {self.TotalPages}",color="white")
        Pagination = Row([PreviousButton, PageLabel, NextButton], alignment=MainAxisAlignment.CENTER)
        return Pagination
   
    # handle on move forward events

    async def GetNextPage(self,e):
        if self.CurrentPage > 0 and ((self.CurrentPage+10)/10) < self.TotalPages:
            self.CurrentPage = self.CurrentPage + 10
            await self.GetScreenChange()

    # handle back move events 

    async def GetPreviousPage(self,e):
        if self.CurrentPage > 10 and (self.CurrentPage/10) < self.TotalPages:
            self.CurrentPage = self.CurrentPage - 10
            await self.GetScreenChange()

