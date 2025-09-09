
from flet import *
import SYSConnectToServers as CS
import pandas as pd
import GUIForm as GF
import GUIBordereaux as Brdx
import GUIBrdxVariablesEdit as GVE


class BrdxVariables(Container):
    def __init__(self, page, download, username):
        super().__init__()

        self.page = page
        self.Download = download
        self.UserName = username
        self.ODS = CS.ConnectToODSServer()
        self.ColumnNames = []; self.RowsData = []; self.RowValues = ''
        self.ScreenName = 'CONIDScreen'; self.ReportTitle = 'Bordereaux Templates'; self.ColumnSelect = 'CONID'
        self.Query = f"select distinct CONID, PremiumCategory from RESVBrdxReportTemplates"
        self.TableData = pd.DataFrame(self.ODS.qryODSGetData(self.Query)); self.ODS.ODSConnection.close
        self.GetReportScreen()
        self.ReportingScreen = Column(expand=True, controls=[self.ReportScreen])

    # generate brdx variables screen     

    def GetReportScreen(self):
        self.GetTableData()
        print(self.ScreenName)
        self.ADDNew = ElevatedButton(on_click=self.GetAddNewProduct,bgcolor="#AD1457",color="white",content=Row(controls=[Icon(name=icons.ADD_ROUNDED,size=12,
            color=colors.WHITE), Text("Add New Product", size=10, weight="bold")]),style=ButtonStyle(shape={"":RoundedRectangleBorder(radius=6)},
            color={"":"white"}),height=30,width=150) 
        self.ReportTableData = DataTable(expand=True,border_radius=8,border=border.all(2,"#263238"),columns=self.ColumnNames,rows=self.RowsData)

        if self.ScreenName == "ProductCodeScreen": 
            self.ReportHeader = Container(expand=True, height=50, bgcolor="#AD1457",border_radius=border_radius.only(top_left=15,top_right=15),
                padding=padding.only(left=15,right=15),content=Row(expand=True,alignment=MainAxisAlignment.SPACE_BETWEEN,
                controls=[IconButton(icon=icons.NAVIGATE_BEFORE_ROUNDED, icon_color=colors.WHITE, on_click = self.GetNavigateBack),
                Container(expand=True,content=Text(self.ReportTitle,text_align="Center",size=20,color="white",weight='bold')),self.ADDNew]))
        else:
            self.ReportHeader = Container(expand=True, height=50, bgcolor="#AD1457",border_radius=border_radius.only(top_left=15,top_right=15),
                padding=padding.only(left=15,right=15),content=Row(expand=True,alignment=MainAxisAlignment.SPACE_BETWEEN,
                controls=[IconButton(icon=icons.NAVIGATE_BEFORE_ROUNDED, icon_color=colors.WHITE, on_click = self.GetNavigateBack),
                Container(expand=True,content=Text(self.ReportTitle,text_align="Center",size=20,color="white",weight='bold'))]))
        
        self.ReportScreen = Column(expand=True,controls=[Row(controls=[Row(expand=True,controls=[self.ReportHeader]),Divider(height=2,
            color="transparent")]),Column(scroll="hidden",expand=True,controls=[Row([self.ReportTableData])])])
        
    # get brdx template table data     

    def GetTableData(self):
        if self.ScreenName == 'CONIDScreen':
            for cols in self.TableData.columns.values: self.ColumnNames.append(DataColumn(Text(cols,size=15,color='#AD1457',weight='bold')))
            for index, rows in self.TableData.iterrows():
                self.RowsData.append(DataRow(cells=[DataCell(Text(rows.values[0], color="wite",size=15)),DataCell(Text(rows.values[1], color="white",size=15))],
                                             on_select_changed = self.GetNavigateFront)) 
                
        elif self.ScreenName == 'ProductCodeScreen':
            for cols in self.TableData.columns.values: self.ColumnNames.append(DataColumn(Text(cols,size=15,color='#AD1457',weight='bold')))
            self.ColumnNames.append(DataColumn(Text("CONID",size=15,color='#AD1457',weight='bold')))
            self.ColumnNames.append(DataColumn(Text("PremiumCategory",size=15,color='#AD1457',weight='bold')))
            self.ColumnNames.append(DataColumn(Text("View",size=15,color='#AD1457',weight='bold')))
            self.ColumnNames.append(DataColumn(Text("Delete",size=15,color='#AD1457',weight='bold')))
            for index, rows in self.TableData.iterrows():
                self.ProductCodes = rows.values[0]
                self.RowsData.append(DataRow(cells=[DataCell(Text(self.ProductCodes, color="wite",size=15)),
                    DataCell(Text(self.CONID, color="wite",size=15)),DataCell(Text(self.PremiumCategory, color="wite",size=15)),
                    DataCell(IconButton(icons.REMOVE_RED_EYE_OUTLINED,icon_color="white",data=rows,icon_size=16,on_click=self.GetNavigateFront)),
                    DataCell(IconButton(icons.DELETE_OUTLINE,icon_color='white',data=[self.CONID, self.PremiumCategory, self.ProductCodes ],
                    icon_size=20, on_click=self.GetAlertMessage))]))          

        else:
            for cols in self.TableData.columns.values: self.ColumnNames.append(DataColumn(Text(cols,size=15,color='#AD1457',weight='bold')))
            for index, rows in self.TableData.iterrows():
                self.RowsData.append(DataRow(cells=[DataCell(Text(value, color="white",size=15))for value in rows.values],on_select_changed = self.GetNavigateFront))  

    # build alert dialog 

    async def GetAlertMessage(self,e):
        data = e.control.data
        self.AlertMessage = AlertDialog(title=Text("Delete Template",weight="bold",color="#AD1457"),content=Text(f"Delete {data[0]}, {data[1]}, {data[2]} Template?"),
            actions=[TextButton("Yes",on_click=self.GetAlertMessageAction,data=data,style=ButtonStyle(shape={"":RoundedRectangleBorder(radius=6)},bgcolor="#AD1457",color="#FAF8F9")),
                     TextButton("No",on_click=self.GetAlertMessageClose,style=ButtonStyle(shape={"":RoundedRectangleBorder(radius=6)},bgcolor="#AD1457",color="#FAF8F9")),],
            actions_alignment=MainAxisAlignment.END,on_dismiss=[])
        await self.ReportingScreen.page.show_dialog_async(self.AlertMessage)

    # handle alert dialog action events

    async def GetAlertMessageAction(self,e):
        await self.ReportingScreen.page.close_dialog_async()
        self.GetDeleteData(e.control.data)
        self.ScreenName = 'ProductCodeScreen'
        await self.GetScreenChange()

    # handle alert dialtog close events 

    async def GetAlertMessageClose(self,e):
        print('im closed')
        await self.ReportingScreen.page.close_dialog_async()

    # handle delete template events  

    def GetDeleteData(self, data):
        print(data)
        self.Query = f"delete from RESVBrdxReportVariables where CONID = '{data[0]}' and PremiumCategory = '{data[1]}' and ProductCode = '{data[2]}'"
        self.ODS.qryODSAppendData(self.Query); self.ODS.ODSConnection.close

    # handle new product code events 

    async def GetAddNewProduct(self, e):
        self.Query = f"SELECT cl.ProductCode FROM dbo.RESVContractLogic cl LEFT JOIN dbo.RESVBrdxReportVariables bv ON cl.ProductCode = bv.ProductCode AND \
        cl.CONID = bv.CONID GROUP BY cl.ProductCode, IIf(bv.ProductCode>'','Yes','No'), cl.CONID HAVING IIf(bv.ProductCode>'','Yes','No')='No' AND \
        cl.CONID ='{self.CONID}' ORDER BY IIf(bv.ProductCode>'','Yes','No') DESC"
        self.ScreenName = 'SelectProductScreen'; self.ColumnSelect = 'ProductCode'; self.ReportTitle = 'Select Product Code'
        self.TableData = pd.DataFrame(self.ODS.qryODSGetData(self.Query)); self.ODS.ODSConnection.close
        self.CurrentPage = 1; self.TotalPages = (self.TableData.shape[0] + 10 - 1) // 10
        await self.GetScreenChange()

    # handle move forward events 
           
    async def GetNavigateFront(self, e):
        if self.ScreenName == 'ProductCodeScreen': 
            self.ProductCode = e.control.data.values[0]
        else:
            self.RowValues = ''
            for value in e.control.cells:
                self.RowValues = str(value.content.value)
        if self.ScreenName == 'CONIDScreen': 
            self.CONID = e.control.cells[0].content.value; self.PremiumCategory = e.control.cells[1].content.value;self.GetProductCode()    
        elif self.ScreenName == 'ProductCodeScreen': self.GetBrdxVariables()
        elif self.ScreenName == 'AddNewProduct': self.ProductCode = self.RowValues; self.GetAddNewProduct()
        elif self.ScreenName == 'SelectProductScreen': self.ProductCode = self.RowValues; self.GetSelectProduct()
        self.TableData = pd.DataFrame(self.ODS.qryODSGetData(self.Query)); self.ODS.ODSConnection.close
        self.CurrentPage = 1; self.TotalPages = (self.TableData.shape[0] + 10 - 1) // 10
        print(self.ScreenName)
        await self.GetScreenChange()

    # handle move back events 

    async def GetNavigateBack(self, e):
        print(self.ScreenName)
        if self.ScreenName == 'CONIDScreen': self.ScreenName = 'MainScreen'
        elif self.ScreenName == 'ProductCodeScreen': self.GetCONID()
        elif self.ScreenName == 'AddNewProduct': self.GetCONID()
        elif self.ScreenName == 'BrdxVariablesScreen': self.GetProductCode()
        elif self.ScreenName == 'SelectProductScreen': self.GetProductCode()
        self.TableData = pd.DataFrame(self.ODS.qryODSGetData(self.Query)); self.ODS.ODSConnection.close
        self.CurrentPage = 1; self.TotalPages = (self.TableData.shape[0] + 10 - 1) // 10
        await self.GetScreenChange()

    # handle screen change events 

    async def GetScreenChange(self):
        self.ColumnNames.clear(); self.RowsData.clear()
        if self.ScreenName == 'BrdxVariablesScreen': self.ReportScreen = GF.FormScreen(self.page, self.Download, self.UserName, self.Query, self.ScreenName, self.ReportTitle).GetFormScreen()
        elif self.ScreenName == 'SelectedProductScreen': self.ReportScreen = GVE.FormEditScreen(self.page, self.Download, self.UserName, self.Query, self.ScreenName, self.ReportTitle).GetFormEditScreen()
        elif self.ScreenName == 'MainScreen': self.ReportScreen = Brdx.Bordereaux(self.page, self.Download, self.UserName).ReportingScreen
        else: self.GetReportScreen()
        self.ReportingScreen.controls.clear()
        self.ReportingScreen.controls.append(self.ReportScreen)
        await self.ReportingScreen.update_async()

    # build brdx variable CONID query    

    def GetCONID(self):
        self.Query = f"select Distinct CONID, PremiumCategory from RESVBrdxReportTemplates"
        self.ScreenName = 'CONIDScreen'; self.ColumnSelect = 'CONID'; self.ReportTitle = 'Bordereaux Templates'

    # build brdx variable premium category query 

    def GetProductCode(self):
        print(self.CONID)
        self.Query = f"Select Distinct ProductCode from RESVBrdxReportVariables where CONID = '{self.CONID}' and PremiumCategory = '{self.PremiumCategory}'"
        self.ScreenName = 'ProductCodeScreen'; self.ColumnSelect = 'ProductCode';  self.ReportTitle = f"{self.CONID} - Bordereaux Products"

    # build brdx variable product code query 

    def GetBrdxVariables(self):
        print(self.CONID, self.ProductCode)
        self.Query = f"Select * from RESVBrdxReportVariables where CONID = '{self.CONID}' and ProductCode = '{self.ProductCode}' and Status = 'Activated'"
        self.ScreenName = 'BrdxVariablesScreen';  self.ReportTitle = f"{self.CONID} - {self.ProductCode} - Bordereaux Variables"

    # build brdx variable blank template query 

    def GetSelectProduct(self):
        print(self.CONID, self.ProductCode)
        self.Query = f"Select *, '' as ProductCode, '' as TableName, '' as Variables from RESVBrdxReportTemplates where CONID = '{self.CONID}' and Status = 'Activated'"
        self.ScreenName = 'SelectedProductScreen';  self.ReportTitle = f"{self.CONID} - {self.ProductCode} - Bordereaux Variables"

