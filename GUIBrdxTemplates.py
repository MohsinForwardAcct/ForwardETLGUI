
from flet import *
import SYSConnectToServers as CS
import pandas as pd
import GUIForm as GF
import GUIBordereaux as Brdx
import GUIBrdxTemplatesEdit as GTE

class BrdxTemplates(Container):

    def __init__(self, page, download, username):
        super().__init__()

        self.page = page
        self.Download = download
        self.UserName = username
        self.ODS = CS.ConnectToODSServer()
        # self.Load = CS.LoadDataToODS()
        self.ColumnNames = []; self.RowsData = []; self.RowValues = ''
        self.ScreenName = 'CONIDScreen'; self.ReportTitle = 'Bordereaux Templates'
        self.Query = f"select distinct CONID, PremiumCategory from RESVBrdxReportTemplates"
        self.TableData = pd.DataFrame(self.ODS.qryODSGetData(self.Query)); self.ODS.ODSConnection.close
        self.GetReportScreen()
        self.ReportingScreen = Column(expand=True, controls=[self.ReportScreen])      

    # generate brdx template screen 

    def GetReportScreen(self):
        self.GetTableData()
        self.ADDNew = ElevatedButton(on_click=self.GetAddNewTemplate,bgcolor="#AD1457",color="white",content=Row(controls=[Icon(name=icons.ADD_ROUNDED,size=12, color=colors.WHITE),
            Text("Add New Template", size=10, weight="bold")]),style=ButtonStyle(shape={"":RoundedRectangleBorder(radius=6)},color={"":"white"}),height=30,width=150)
        self.ReportTableData = DataTable(expand=True,border_radius=8,border=border.all(2,"#263238"),columns=self.ColumnNames,rows=self.RowsData)
        self.ReportHeader = Container(expand=True, height=50, bgcolor="#AD1457",border_radius=border_radius.only(top_left=15,top_right=15),
            padding=padding.only(left=15,right=15),content=Row(expand=True,alignment=MainAxisAlignment.SPACE_BETWEEN,controls=
            [IconButton(icon=icons.NAVIGATE_BEFORE_ROUNDED, icon_color=colors.WHITE, on_click = self.GetNavigateBack),Container(expand=True,content=
            Text(self.ReportTitle,text_align="Center",size=20,color="white",weight='bold')),self.ADDNew]))
        self.ReportScreen = Column(expand=True,controls=[Row(controls=[Row(expand=True,controls=[self.ReportHeader]),Divider(height=2,
            color="transparent")]),Column(scroll="hidden",expand=True,controls=[Row([self.ReportTableData])])])

    # get brdx template table data     

    def GetTableData(self):
        if self.ScreenName == 'CONIDScreen':
            for cols in self.TableData.columns.values: self.ColumnNames.append(DataColumn(Text(cols,size=15,color='#AD1457',weight='bold')))
            self.ColumnNames.append(DataColumn(Text("View",size=15,color='#AD1457',weight='bold')))
            self.ColumnNames.append(DataColumn(Text("Delete",size=15,color='#AD1457',weight='bold')))
            for index, rows in self.TableData.iterrows():
                self.CONID = rows.values[0]; self.PremiumCategory = rows.values[1]
                self.RowsData.append(DataRow(cells=[DataCell(Text(self.CONID, color="wite",size=15)),DataCell(Text(self.PremiumCategory, color="white",size=15)),
                    DataCell(IconButton(icons.REMOVE_RED_EYE_OUTLINED,icon_color='white',data=rows,icon_size=16,on_click=self.GetNavigateFront)),
                    DataCell(IconButton(icons.DELETE_OUTLINE,icon_color='white',data=[self.CONID, self.PremiumCategory],icon_size=20, on_click=self.GetAlertMessage))]))  
        else:
            for cols in self.TableData.columns.values: self.ColumnNames.append(DataColumn(Text(cols,size=15,color='#AD1457',weight='bold')))
            for index, rows in self.TableData.iterrows():
                self.RowsData.append(DataRow(cells=[DataCell(Text(value, color="white",size=15))for value in rows.values],on_select_changed = self.GetNavigateFront))  

    # build alert dialog 

    async def GetAlertMessage(self,e):
        data = e.control.data
        self.AlertMessage = AlertDialog(title=Text('Delete Template',weight="bold",color="#AD1457"),content=Text(f"Delete {data[0]}, {data[1]} Template, All Products will be deleted?"),
            actions=[TextButton("Yes",on_click=self.GetAlertMessageAction,data=data,style=ButtonStyle(shape={"":RoundedRectangleBorder(radius=6)},bgcolor="#AD1457",color="#FAF8F9")),
                     TextButton("No",on_click=self.GetAlertMessageClose,style=ButtonStyle(shape={"":RoundedRectangleBorder(radius=6)},bgcolor="#AD1457",color="#FAF8F9")),],
            actions_alignment=MainAxisAlignment.END,on_dismiss=[])
        await self.ReportingScreen.page.show_dialog_async(self.AlertMessage)

    # handle alert dialog action events

    async def GetAlertMessageAction(self,e):
        await self.ReportingScreen.page.close_dialog_async()
        self.GetDeleteData(e.control.data)
        self.ScreenName = 'CONIDScreen'; self.GetCONID()
        await self.GetScreenChange()
        
    # handle alert dialtog close events 

    async def GetAlertMessageClose(self,e):
        print('im closed')
        await self.ReportingScreen.page.close_dialog_async()

    def GetAlertMessage2(self, Highlight, Commentry):
        self.AlertMessage = AlertDialog(title=Text(Highlight,weight="bold",color="#AD1457"),content=Text(Commentry),
            actions=[TextButton("Ok",on_click=self.GetAlertMessageClose,style=ButtonStyle(shape={"":RoundedRectangleBorder(radius=6)},
            bgcolor="#AD1457",color="white"))],actions_alignment=MainAxisAlignment.END,on_dismiss=[])

    # handle delete template events 

    def GetDeleteData(self, data):
        print(data)
        self.Query = f"delete from RESVBrdxReportTemplates where CONID = '{data[0]}' and PremiumCategory = '{data[1]}'"
        self.ODS.qryODSAppendData(self.Query); self.ODS.ODSConnection.close
        self.Query = f"delete from RESVBrdxReportVariables where CONID = '{data[0]}' and PremiumCategory = '{data[1]}'"
        self.ODS.qryODSAppendData(self.Query); self.ODS.ODSConnection.close

    # hanlde file upload events 

    async def GetFileUpload(self):
        print(self.CONID, self.PremiumCategory)
        self.Query=f"select * from RESVBrdxReportTemplates where CONID = '{self.CONID}' and PremiumCategory = '{self.PremiumCategory}' and Status = 'Activated'"
        ValidateData = pd.DataFrame(self.ODS.qryODSGetData(self.Query)); self.ODS.ODSConnection.close
        print(ValidateData.shape[0])
        if ValidateData.shape[0] > 0: 
            self.GetAlertMessage2('ADD New Template Failed', f'Template already exist for {self.CONID} & {self.PremiumCategory}')
            await self.ReportingScreen.page.show_dialog_async(self.AlertMessage)   
        else:
            self.ScreenName="AddNewTemplate"; self.ReportTitle = f"{self.CONID} - {self.PremiumCategory} - Add New Template"; 
            self.ReportScreen = GTE.FormEditScreen(self.page, self.Download, self.UserName, self.Query, self.ScreenName, self.ReportTitle).GetFormEditScreen()
            self.ReportingScreen.controls.clear()
            self.ReportingScreen.controls.append(self.ReportScreen)
            await self.ReportingScreen.update_async()

    # handle add new template events 

    async def GetAddNewTemplate(self, e):
        self.ScreenName = 'AddNewScreen'; self.ColumnSelect = 'CONID'; self.ReportTitle = 'Select CONID'
        self.Query = f"select distinct CONID from RESVContractLogic"
        self.TableData = pd.DataFrame(self.ODS.qryODSGetData(self.Query)); self.ODS.ODSConnection.close
        self.CurrentPage = 1; self.TotalPages = (self.TableData.shape[0] + 10 - 1) // 10
        await self.GetScreenChange()

    # handle move forward events 
           
    async def GetNavigateFront(self, e):
        print(self.ScreenName)
        if self.ScreenName == 'PremiumCategoryScreen': 
            for value in e.control.cells: self.PremiumCategory = value.content.value
            await self.GetFileUpload()
        else:
            if self.ScreenName == 'CONIDScreen': 
                self.CONID = e.control.data.values[0]
                self.PremiumCategory = e.control.data.values[1]
                self.GetCONID()
            else:
                self.RowValues = ''
                for value in e.control.cells:
                    self.RowValues = str(value.content.value)
                if self.ScreenName == 'AddNewScreen': self.CONID = self.RowValues; self.GetPremiumCategory()
            self.TableData = pd.DataFrame(self.ODS.qryODSGetData(self.Query)); self.ODS.ODSConnection.close
            self.CurrentPage = 1; self.TotalPages = (self.TableData.shape[0] + 10 - 1) // 10
            await self.GetScreenChange()

    # handle move back events 

    async def GetNavigateBack(self, e):
        if self.ScreenName == 'CONIDScreen': self.ReportScreen = Brdx.Bordereaux(self.page, self.Download, self.UserName).ReportingScreen
        elif self.ScreenName == 'AddNewScreen': self.ReportScreen = BrdxTemplates(self.page, self.Download, self.UserName).ReportingScreen
        elif self.ScreenName == 'PremiumCategoryScreen': self.ReportScreen = BrdxTemplates(self.page, self.Download, self.UserName).ReportingScreen
        self.ReportingScreen.controls.clear()
        self.ReportingScreen.controls.append(self.ReportScreen)
        await self.ReportingScreen.update_async()

    # handle screen change events 

    async def GetScreenChange(self):
        self.ColumnNames.clear(); self.RowsData.clear()
        if self.ScreenName == 'BrdxTemplates': self.ReportScreen = GF.FormScreen(self.page, self.Download, self.UserName, self.Query, self.ScreenName, self.ReportTitle).GetFormScreen()
        # elif self.ScreenName == 'PremiumCategoryScreen': 
        else: self.GetReportScreen()
        self.ReportingScreen.controls.clear()
        self.ReportingScreen.controls.append(self.ReportScreen)
        await self.ReportingScreen.update_async()

    # build brdx template CONID query

    def GetCONID(self):
        self.Query = f"Select * from RESVBrdxReportTemplates where CONID = '{self.CONID}' and Status = 'Activated' order by ColumnSequence"
        self.ScreenName = 'BrdxTemplates'; self.ReportTitle = f"{self.CONID} - Bordereaux Template"

    # build brdx templete premium category query 

    def GetPremiumCategory(self):
        self.Query=f"select distinct pl.PremiumCategory from RESVProductLogic pl left join RESVContractLogic cl on cl.ProductCode = pl.ProductCode \
            where cl.CONID = '{self.CONID}' order by PremiumCategory"
        self.ColumnSelect = 'PremiumCategory'; self.ScreenName = 'PremiumCategoryScreen'
        self.ReportTitle = f'{self.CONID} - Add New Template'

