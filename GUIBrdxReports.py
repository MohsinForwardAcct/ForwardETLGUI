
from flet import *
import SYSConnectToServers as CS
import pandas as pd
import GUIBordereaux as Brdx
import GUIBrdxExtract as GBE
import GUIBrdxForm as GBF

class BrdxReports(Container):
    def __init__(self, page, download, username):
        super().__init__()

        self.page = page
        self.Download = download
        self.UserName = username
        self.ETL = CS.ConnectToETLServer()
        self.ODS = CS.ConnectToODSServer()
        self.ColumnNames = []; self.RowsData = []; self.RowValues = ''
        self.ColumnSelect = 'CONID'; self.ReportTitle = 'Bordereaux Reports'; self.ScreenName = 'CONIDScreen'
        self.GetBrdxCONID()
        self.Query = f"select distinct CONID from FACTData where CONID in ({self.BrdxCONID}) Order by CONID"
        self.TableData = pd.DataFrame(self.ETL.qryETLGetData(self.Query)); self.ETL.ETLConnection.close
        self.GetReportScreen()
        self.ReportingScreen = Column(expand=True, controls=[self.ReportScreen])

    # get brdx CONID template

    def GetBrdxCONID(self):
        self.Query = f"select Distinct CONID from RESVBrdxReportTemplates"
        self.BrdxTemplates = pd.DataFrame(self.ODS.qryODSGetData(self.Query)); self.ODS.ODSConnection.close
        self.BrdxCONID = f"'Test'"
        if self.BrdxTemplates.shape[0] > 0:
            for idx, CONID in self.BrdxTemplates.iterrows(): self.BrdxCONID = self.BrdxCONID + f",'{CONID.values[0]}'"

    # create reportFormScreen and option buttons 

    def GetReportScreen(self):
        self.GetTableData()
        self.ReportTableData = DataTable(expand=True,border_radius=8,border=border.all(2,"#263238"),columns=self.ColumnNames,rows=self.RowsData)
        self.ReportHeader = Container(expand=True, height=50, bgcolor="#AD1457",border_radius=border_radius.only(top_left=15,top_right=15),
                            padding=padding.only(left=15,right=15),content=Row(expand=True,alignment=MainAxisAlignment.SPACE_BETWEEN,
                            controls=[IconButton(icon=icons.NAVIGATE_BEFORE_ROUNDED, icon_color=colors.WHITE, on_click = self.GetNavigateBack),
                            Container(expand=True,content=Text(self.ReportTitle,text_align="Center",size=20,color="white",weight='bold'))]))
        self.ReportScreen = Column(expand=True,controls=[Row(controls=[Row(expand=True,controls=[self.ReportHeader]),Divider(height=2,
                            color="transparent")]),Column(scroll="hidden",expand=True,controls=[Row([self.ReportTableData])])])

    # get table data build

    def GetTableData(self):
        for cols in self.TableData.columns.values: self.ColumnNames.append(DataColumn(Text(cols,size=15,color='#AD1457',weight='bold')))
        for index, rows in self.TableData.iterrows():
            self.RowsData.append(DataRow(cells=[DataCell(Text(rows[self.ColumnSelect],color="white",size=15,width=700))],
            on_select_changed = self.GetNavigateFront))

    # handle navigate forward events
           
    async def GetNavigateFront(self, e):
        self.RowValues = ''
        for value in e.control.cells:self.RowValues = str(value.content.value)
        if self.ScreenName == 'CONIDScreen': self.CONID = self.RowValues; self.GetReportingYears()
        elif self.ScreenName == 'ReportingYearScreen': self.ReportingYear = self.RowValues; self.GetReportingPeriods()
        elif self.ScreenName == 'ReportingPeriodScreen': self.ReportingPeriod = self.RowValues; self.GetReportingContracts()
        elif self.ScreenName == 'ContractNumberScreen': self.ContractNumber = self.RowValues; self.GetPremiumCategory()
        elif self.ScreenName == 'PremiumCategoryScreen': self.PremiumCategory = self.RowValues; self.GetProductCodes()
        elif self.ScreenName == 'ProductCodeScreen': self.ProductCode = self.RowValues; await self.GetBordereauxReports()
        print(self.Query)
        self.TableData = pd.DataFrame(self.ETL.qryETLGetData(self.Query)); self.ETL.ETLConnection.close
        if self.ScreenName == 'ProductCodeScreen': self.AddVariable()
        self.CurrentPage = 1; self.TotalPages = (self.TableData.shape[0] + 10 - 1) // 10
        await self.GetScreenChange()

    # handle navgiate back events 

    async def GetNavigateBack(self, e):
        if self.ScreenName == 'ReportingYearScreen': self.GetReportingCONID()
        elif self.ScreenName == 'ReportingPeriodScreen': self.GetReportingYears()
        elif self.ScreenName == 'ContractNumberScreen': self.GetReportingPeriods()
        elif self.ScreenName == 'PremiumCategoryScreen': self.GetReportingContracts()
        elif self.ScreenName == 'ProductCodeScreen': self.GetPremiumCategory()     
        print(self.Query)
        self.TableData = pd.DataFrame(self.ETL.qryETLGetData(self.Query)); self.ETL.ETLConnection.close
        self.CurrentPage = 1; self.TotalPages = (self.TableData.shape[0] + 10 - 1) // 10
        if self.ScreenName == 'CONIDScreen': self.ScreenName = 'MainScreen'
        await self.GetScreenChange()

    # handle screen change events 

    async def GetScreenChange(self):
        print(self.ColumnSelect, self.ScreenName)
        self.ColumnNames.clear(); self.RowsData.clear()
        if self.ScreenName == 'MainScreen': self.ReportScreen = Brdx.Bordereaux(self.page, self.Download, self.UserName).ReportingScreen
        elif self.ScreenName == 'GUIBrdxForm': self.ReportScreen = await GBF.BrdxForm(self.page, self.Download, self.UserName, self.BrdxReport, self.ScreenName, self.ReportTitle).GetBrdxReportScreen()
        else: self.GetReportScreen()
        self.ReportingScreen.controls.clear()
        self.ReportingScreen.controls.append(self.ReportScreen)
        await self.ReportingScreen.update_async()

    # build unique CONID query 

    def GetReportingCONID(self):   
        self.GetBrdxCONID()
        self.Query = f"select distinct CONID from FACTData where CONID in ({self.BrdxCONID}) order by CONID"
        self.ColumnSelect = 'CONID'; self.ReportTitle = 'Bordereaux Reports'; self.ScreenName = 'CONIDScreen'

    # build CONID brdx query 

    def GetReportingYears(self):
        self.Query=f"select distinct ReportingYear from FACTData where CONID = '{self.CONID}' order by ReportingYear" 
        self.ColumnSelect = 'ReportingYear'; self.ScreenName = 'ReportingYearScreen'
        self.ReportTitle = f'{self.CONID} - Reporting Years'

    # build CONID reporting year brdx query 

    def GetReportingPeriods(self):
        self.Query=f"select distinct ReportingPeriod from FACTData where CONID = '{self.CONID}' and ReportingYear = '{self.ReportingYear}' order by ReportingPeriod" 
        self.ColumnSelect = 'ReportingPeriod'; self.ScreenName = 'ReportingPeriodScreen'
        self.ReportTitle = f'{self.CONID}-{self.ReportingYear} - Reporting Periods'

    # build CONID reporting year and period brdx query 

    def GetReportingContracts(self):
        self.Query=f"select distinct ContractNumber from FACTData where CONID = '{self.CONID}' and ReportingYear = '{self.ReportingYear}'\
              and ReportingPeriod = '{self.ReportingPeriod}' order by ContractNumber" 
        self.ColumnSelect = 'ContractNumber'; self.ScreenName = 'ContractNumberScreen'
        self.ReportTitle = f'{self.CONID}-{self.ReportingPeriod} - Contrat Numbers'

    # build CONID reporting year, period and contractnumber brdx query 

    def GetPremiumCategory(self):
        self.Query=f"select distinct PremiumCategory from FACTData where CONID = '{self.CONID}' and ReportingYear = '{self.ReportingYear}'\
             and ReportingPeriod = '{self.ReportingPeriod}' and ContractNumber = '{self.ContractNumber}' order by PremiumCategory" 
        self.ColumnSelect = 'PremiumCategory'; self.ScreenName = 'PremiumCategoryScreen'
        self.ReportTitle = f'{self.CONID}-{self.ReportingPeriod}-{self.ContractNumber} - Premium Category'

    # build CONID reporting year, period, contractnumber and productcode brdx query 

    def GetProductCodes(self):
        self.Query=f"select distinct ProductCode from FACTData where CONID = '{self.CONID}' and ReportingYear = '{self.ReportingYear}'\
             and ReportingPeriod = '{self.ReportingPeriod}' and ContractNumber = '{self.ContractNumber}' and PremiumCategory = '{self.RowValues}'\
             order by ProductCode" 
        self.ColumnSelect = 'ProductCode'; self.ScreenName = 'ProductCodeScreen'
        self.ReportTitle = f'{self.CONID}-{self.ReportingPeriod}-{self.ContractNumber}-{self.PremiumCategory} - Product Codes'

    # add multi product options to brdx fetch screen 

    def AddVariable(self):
        NewVariable = pd.DataFrame({'ProductCode': 'ALL Products'},index=[0])
        self.TableData = pd.concat([self.TableData, NewVariable])

    # extract brdx report for filtered options 

    async def GetBordereauxReports(self):
        self.BrdxReport = pd.DataFrame()
        await self.GetScreenUpdate(); await self.GetBrdxExtractProg(self.ProductCode)
        if self.ProductCode == 'ALL Products':
            self.ProductList = pd.DataFrame(self.ETL.qryETLGetData(self.Query)); self.ETL.ETLConnection.close; await self.GetScreenUpdate()
            for index, Product in self.ProductList.iterrows():
                print(Product.values[0]); await self.GetBrdxExtractProg(Product.values[0]); await self.GetScreenUpdate()
                self.BrdxData = await GBE.DownloadBrdxReport().GetBrdxDownloadData(self.ReportingYear, self.ReportingPeriod, self.CONID, self.PremiumCategory, Product.values[0], self.ContractNumber)
                self.GetBrdxReportConcat()
        else:
            await self.GetScreenUpdate()
            self.BrdxData = await GBE.DownloadBrdxReport().GetBrdxDownloadData(self.ReportingYear, self.ReportingPeriod, self.CONID, self.PremiumCategory, self.ProductCode, self.ContractNumber)
            self.BrdxReport = self.BrdxData[0]
        self.ScreenName = 'GUIBrdxForm'
        self.ReportTitle = f'{self.ContractNumber}-{self.ProductCode}-{self.CONID}-{self.PremiumCategory}-{self.ReportingPeriod}'

    def GetBrdxReportConcat(self):
        if self.BrdxReport.shape[0] == 0: 
            self.BrdxReport = self.BrdxData[1]
            self.BrdxReport = pd.concat([self.BrdxReport, self.BrdxData[0]])
            self.BrdxReport = self.BrdxReport[self.BrdxReport['Template'] != 'Template']
            self.BrdxReport = self.BrdxReport.drop(columns=['Template']) 
        else: self.BrdxReport = pd.concat([self.BrdxReport, self.BrdxData[0]])

    # get progress status data

    async def GetBrdxExtractProg(self, ProductCode):
        data = DataRow(cells=[DataCell(Text(ProductCode,color="white",size=15,width=700)),
                DataCell(Text(f'Extracting {ProductCode} data......',color="#51B613",size=15,width=700)),
                DataCell(ProgressBar(aspect_ratio=5.0,height=10,width=10,color="#64DD17"))])
        self.table.rows.append(data)
        await self.table.update_async()

    # build progress status screen

    def GetBrdxSelectScreen(self):
        self.GetTableData2()
        self.ReportHeader = Container(expand=True, height=50, bgcolor="#AD1457",border_radius=border_radius.only(top_left=15,top_right=15),
                            padding=padding.only(left=15,right=15),content=Row(expand=True,alignment=MainAxisAlignment.SPACE_BETWEEN,
                            controls=[IconButton(icon=icons.NAVIGATE_BEFORE_ROUNDED, icon_color=colors.WHITE, on_click = self.GetNavigateBack),
                            Container(expand=True,content=Text(self.ReportTitle,text_align="Center",size=20,color="white",weight='bold'))]))
        self.ReportScreen = Column(expand=True,controls=[Row(controls=[Row(expand=True,controls=[self.ReportHeader]),Divider(height=2,
                            color="transparent")]),Column(scroll="hidden",expand=True,controls=[Row([self.table])])])

    # update progress status screen

    async def GetScreenUpdate(self):
        self.ColumnNames.clear(); self.RowsData.clear()
        self.GetBrdxSelectScreen()
        self.ReportingScreen.controls.clear()
        self.ReportingScreen.controls.append(self.ReportScreen)
        await self.ReportingScreen.update_async()   

    # get progress status table data

    def GetTableData2(self):
        self.table = DataTable(expand= True, columns=[DataColumn(Text('Product Code',size=18,color='#AD1457',weight='bold')),
                                                      DataColumn(Text("Bordereaux Extraction Progress", weight="bold", color="#AD1457", size=18)),
                    DataColumn(Text("Status", weight="bold", color="#AD1457", size=18),numeric=True)],width=1200, heading_row_height= 75,
                    data_row_max_height= 50)  

