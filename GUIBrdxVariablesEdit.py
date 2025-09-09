
from flet import *
import SYSConnectToServers as CS
import pandas as pd
import GUIForm as GF
from datetime import datetime

class FormEditScreen(Container):

    def __init__(self, page, download, username, Query, ScreenName, ReportTitle):
        super().__init__()

        self.page = page
        self.Download = download
        self.ODS = CS.ConnectToODSServer()
        self.UserName = username
        self.TimeStamp = datetime.now()
        self.ColumnNames = []; self.RowsData = []; self.RowValues = ''
        self.Query = Query; self.ScreenName = ScreenName; self.ReportTitle = ReportTitle; self.ColumnSelect = ScreenName
        self.TableList = pd.DataFrame(self.ODS.qryODSGetData('select distinct TableName from RESVVariablesLogic')); self.ODS.ODSConnection.close
        self.VariableList = pd.DataFrame(self.ODS.qryODSGetData('select distinct ColumnName from RESVVariablesLogic')); self.ODS.ODSConnection.close
        self.TableData = pd.DataFrame(self.ODS.qryODSGetData(self.Query)); self.ODS.ODSConnection.close
        self.TemplateData = self.TableData.head(1).reset_index(drop=True)
        self.BrdxTemplateID = self.TableData['BrdxTemplateID'].values[0]
        self.TableData = self.TableData.drop(['Status','BrdxVariableID', 'BrdxTemplateID'], axis=1)
        self.ProductCode = self.TemplateData['ProductCode'].values[0]
        if self.ScreenName == 'SelectedProductScreen': self.FormatTableData()
        self.FormScreen = Column(expand=True, controls=[])
        self.TableName = ''; self.Variables = ''
        self.CONID = self.TemplateData['CONID'].values[0]; self.PremiumCategory = self.TemplateData['PremiumCategory'].values[0] 

    def FormatTableData(self):
        self.ProductCode = self.ReportTitle.split('-')[1].strip()
        self.TableData.loc[self.TableData.index, 'ProductCode'] = self.ProductCode
        self.TableData = self.TableData[['UID','CONID','ProductCode', 'PremiumCategory', 'ColumnOutput','TableName','Variables']]

    # create addFormScreen and option buttons 
        
    def GetFormEditScreen(self):
        self.GetFormTable()
        self.FormFields = Container(expand=True,height=150,bgcolor="white10",border=border.all(1,"#ebebeb"),border_radius=8,padding=15,
                        content=Column(expand=True,controls=[Row(expand=True,controls=self.GetFormFields()),
                        Divider(height=2,color="transparent"),Row(expand=True,alignment=MainAxisAlignment.END,controls=[self.GetFormButton()])]))
        self.FormHeader = Container(expand=True, height=50, bgcolor="#AD1457",border_radius=border_radius.only(top_left=15,top_right=15),
                        padding=padding.only(left=15,right=15),content=Row(expand=True,alignment=MainAxisAlignment.SPACE_BETWEEN,
                        controls=[Container(expand=True,content=Text(self.ReportTitle,text_align="left",size=18,color="white",weight='bold'))]))  
        self.ReportScreen = Column(expand=True,controls=[Row(expand = False, controls=[self.FormHeader]), Row(expand = False, controls=[self.FormFields]),self.FormTable])
        self.FormScreen.controls.append(self.ReportScreen)
        return self.FormScreen
    
    def GetFormTable(self):
        for cols in self.TableData.columns.values: self.ColumnNames.append(DataColumn(Text(cols,size=15,color='#AD1457',weight='bold')))
        for index, rows in self.TableData.iterrows():
            self.RowsData.append(DataRow(cells=[DataCell(Container(
                Text(value, color="white",size=13)))for value in rows.values],on_select_changed =self.PageLogicTables))  
        self.FormTableData = DataTable(expand=True, border=border.all(2,"#ebebeb"), border_radius=10, columns=self.ColumnNames, rows=self.RowsData)
        self.FormTable =  Column(expand=True, controls=[ResponsiveRow([self.FormTableData])],scroll='always')

    async def GetFormScreenUpdate(self):
        self.ColumnNames.clear(); self.RowsData.clear()
        self.GetFormEditScreen()
        self.FormScreen.controls.clear()
        self.FormScreen.controls.append(self.ReportScreen)
        await self.FormScreen.update_async()

    async def PageLogicTables(self,e):
        counter = 0; valuecounter = 3
        for Value in self.FormFields.content.controls[0].controls:
            self.ObjectType = str(self.FormFields.content.controls[0].controls[counter].content.controls[0])
            self.FieldName = self.FormFields.content.controls[0].controls[counter].content.controls[0].value
            self.FieldData = e.control.cells[counter].content.content.value
            # print(self.ObjectType, self.FieldName, self.FieldData)
            if self.ObjectType[:4] == 'text': 
                # if self.FieldName == 'TableName' and self.FieldData == '': print('this is test')
                self.FormFields.content.controls[0].controls[counter].content.controls[1].value = self.FieldData
            counter = counter + 1
        await self.FormFields.update_async()
    
    # create add form fields
    
    def GetFormFields(self):
        TableFrameColumns = self.TableData.columns; ColumnNames = []; counter = 0
        EditList = ['TableName','Variables']
        for cols in TableFrameColumns:
            self.ColumnName = cols; self.Ability = 'True'
            if self.ColumnName in EditList: self.Ability = 'False'
            if self.ColumnName == 'TableName':
                ColumnNames.append(Container(expand=2,height=45,bgcolor="#ebebeb",border_radius=6,padding=8,content=Column(spacing=1,controls=[
                    Dropdown(on_change=self.DropdownValue,label=f'{self.ColumnName}',color="white",border_color="transparent",height=20,text_size=13,
                    options=self.GetDropDownOptions()), TextField(border_color="transparent",color="black",height=15,text_size=13,value=self.TableName,disabled='True')])))            
            elif self.ColumnName == 'Variables':
                ColumnNames.append(Container(expand=2,height=45,bgcolor="#ebebeb",border_radius=6,padding=8,content=Column(spacing=1,controls=[
                    Dropdown(on_change=self.DropdownValue,label=f'{self.ColumnName}',color="white",border_color="transparent",height=20,text_size=13,
                    options=self.GetDropDownOptions()), TextField(border_color="transparent",color="black",height=15,text_size=13,value=self.Variables,disabled='True')])))          
            else:    
                ColumnNames.append(Container(expand=2,height=45,bgcolor="#ebebeb",border_radius=6,padding=8,content=Column(spacing=1,controls=[
                    Text(value=f'{self.ColumnName}',size=9,color="black",weight="bold"),TextField(border_color="transparent",height=20,text_size=13,disabled=self.Ability,
                    content_padding=0,cursor_color="black",cursor_width=1,cursor_height=18,color="black")])))
            counter = counter + 1
        return ColumnNames

    async def DropdownValue(self,e):
        print(self.ColumnName, e.control.label, e.control.value)
        if e.control.value in self.TableList.values: 
            self.TableName = e.control.value
            options = self.GetDropDownOptions()
            self.FormFields.content.controls[0].controls[5].content.controls[1].value = self.TableName 
            self.FormFields.content.controls[0].controls[6].content.controls[0].options.clear()
            self.FormFields.content.controls[0].controls[6].content.controls[0].options = options
        elif e.control.value in self.VariableList.values: 
            self.Variables = e.control.value
            self.FormFields.content.controls[0].controls[6].content.controls[1].value = self.Variables
        elif e.control.value == 'Function': 
            self.Variables = e.control.value
            self.FormFields.content.controls[0].controls[5].content.controls[1].value = 'Function'
            self.FormFields.content.controls[0].controls[6].content.controls[1].disabled = 'False'
        await self.FormFields.content.update_async()
        
    def GetDropDownOptions(self):
        # self.ProductCode = str(self.ReportTitle).split("-")[1].strip()
        print(self.ColumnName, self.TableName, self.Variables)
        if self.ColumnName == 'TableName': 
            self.OptionQuery = f"select distinct TableName from RESVVariablesLogic where {self.ProductCode} = 'Activate'"
        elif self.ColumnName == 'Variables': 
            self.OptionQuery = f"select distinct ColumnName from RESVVariablesLogic where {self.ProductCode} = 'Activate' and TableName = '{self.TableName}'"
        self.OptionsData = pd.DataFrame(self.ODS.qryODSGetData(self.OptionQuery)).values; self.ODS.ODSConnection.close
        options = []
        for self.FieldValue in self.OptionsData:
            options.append(dropdown.Option(self.FieldValue[0]))
        if self.ColumnName == 'TableName': options.append(dropdown.Option('Function'))
        return options

    async def UpdateValue(self,e):
        self.UID = self.FormFields.content.controls[0].controls[0].content.controls[1].value 
        self.TableName = self.FormFields.content.controls[0].controls[5].content.controls[1].value
        self.Variables = self.FormFields.content.controls[0].controls[6].content.controls[1].value 
        print(self.UID, self.Variables, self.TableName)
        if self.Variables == 'NUL': self.Variables = None
        self.TableData.loc[self.TableData['UID'] == self.UID, 'TableName'] = self.TableName
        self.TableData.loc[self.TableData['UID'] == self.UID, 'Variables'] = self.Variables
        await self.GetFormScreenUpdate()    
    
    # create add form buttons

    def GetFormButton(self):
        FormButton = Container(alignment=alignment.center,content= Row(controls=[
            ElevatedButton(on_click=self.GetActivated,disabled='True',bgcolor="#AD1457",color="white",content=Row(controls=[Icon(name=icons.START_ROUNDED,size=12, color=colors.WHITE),
                    Text("Activate", size=10, weight="bold")]),style=ButtonStyle(shape={"":RoundedRectangleBorder(radius=6)},color={"":"white"}),height=30,width=100),
            ElevatedButton(on_click=self.GetValidate,bgcolor="#AD1457",color="white",content=Row(controls=[Icon(name=icons.DONE_ROUNDED,size=12, color=colors.WHITE),
                    Text("Validate", size=10, weight="bold")]),style=ButtonStyle(shape={"":RoundedRectangleBorder(radius=6)},color={"":"white"}),height=30,width=100),
            ElevatedButton(on_click=self.UpdateValue,bgcolor="#AD1457",color="white",content=Row(controls=[Icon(name=icons.UPDATE_ROUNDED,size=12, color=colors.WHITE),
                    Text("Update", size=10, weight="bold")]),style=ButtonStyle(shape={"":RoundedRectangleBorder(radius=6)},color={"":"white"}),height=30,width=100),
            Switch(label='Edit Mode',on_change=self.GetEditModeOff, active_color="#AD1457",focus_color="#AD1457",label_position=LabelPosition.LEFT,value=True)]))
        return FormButton
    
    async def GetActivated(self, e):
        self.ActionCall = 'ActivateVariables'
        await self.GetAlertMessage('Activate Brdx Variables','Brdx Variables will replace the existing Variables, to proceed click Yes')

    async def GetActivateTemplateAction(self):
        self.TableData.loc[self.TableData.index,'Status'] = 'Activated'
        query = f"Update RESVBrdxReportVariables SET Status = 'Deactivated' where CONID = '{self.CONID}' and PremiumCategory = '{self.PremiumCategory}' \
            and ProductCode = '{self.ProductCode}'"
        self.UserID = self.ODS.qryODSGetData(f"Select UserID from ETLUserData where UserName = '{self.UserName}'").values[0][0]
        self.TimeID = (str(self.TimeStamp).split(" "))[1].replace(":","").replace(".","")
        self.ReportID = str(self.CONID) + str(self.PremiumCategory[:4]) + str(self.UserID) + self.TimeID 
        self.TableData.loc[self.TableData.index,'BrdxTemplateID'] = self.BrdxTemplateID 
        self.TableData.loc[self.TableData.index,'BrdxVariableID'] = self.ReportID
        self.ODS.qryODSAppendData(query); self.GetLogReport()
        self.TableData = self.TableData.drop(['UID'], axis=1)
        CS.LoadDataToODS().LoadDataToODS(self.TableData, 'RESVBrdxReportVariables', 'ODS')
        CS.LoadDataToODS().LoadDataToODS(self.LogFrame, 'SYSLogReports', 'ODS')


    def GetLogReport(self):
        self.LogFrame = pd.DataFrame({'ReportName':'RESVBrdxReportTemplates', 'ReportID':self.ReportID, 'CONID':self.CONID, 'PremiumCategory':self.PremiumCategory,
                                 'UserName':self.UserName, 'TimeStamp':self.TimeStamp},index=[0])

    async def GetValidate(self, e):
        Validation1 = 'Fail'; Validation2 = 'Fail'
        Test1 = self.TableData['TableName'].values; Test2 = self.TableData['Variables'].values
        if None not in Test1 and '' not in Test1: Validation1 = 'Pass'
        if None not in Test2 and '' not in Test2: Validation2 = 'Pass'
        if Validation1 == 'Pass' and Validation2 == 'Pass': await self.GetValidationPass() 
        elif Validation1 == 'Pass' and Validation2 == 'Fail':
            Validation3 = self.TableData[self.TableData['Variables'] == '']
            Validation3 = Validation3[Validation3['TableName'] != 'DIMManualData']
            if Validation3.shape[0] == 0: await self.GetValidationPass()
            else: await self.GetValidationFail()    
        else: await self.GetValidationFail()


    async def GetValidationPass(self):
        self.FormFields.content.controls[2].controls[0].content.controls[0].disabled = 'False'
        await self.FormFields.update_async() 

    async def GetValidationFail(self):
        self.GetAlertMessage2('Valiation Failed', f'TableName and Non Manaul fields cannot be blank!')
        await self.FormScreen.page.show_dialog_async(self.AlertMessage)  


    def GetAlertMessage2(self, Highlight, Commentry):
        self.AlertMessage = AlertDialog(title=Text(Highlight,weight="bold",color="#AD1457"),content=Text(Commentry),
            actions=[TextButton("Ok",on_click=self.GetAlertMessageClose,style=ButtonStyle(shape={"":RoundedRectangleBorder(radius=6)},
            bgcolor="#AD1457",color="white"))],actions_alignment=MainAxisAlignment.END,on_dismiss=[])
    
    async def GetEditModeOff(self,e):
        self.ScreenName = 'BrdxVariablesScreen'
        self.ReportScreen = GF.FormScreen(self.page, self.Download, self.UserName, self.Query, self.ScreenName, self.ReportTitle).GetFormScreen()
        self.FormScreen.controls.clear()
        self.FormScreen.controls.append(self.ReportScreen)
        await self.FormScreen.update_async()

    async def GetAlertMessageAction(self,e):
        await self.FormScreen.page.close_dialog_async()
        await self.GetFormScreenUpdate()
        print(self.ActionCall)
        if self.ActionCall == 'DeleteLine': await self.GetDeleteLineAction()
        elif self.ActionCall == 'ActivateVariables': await self.GetActivateTemplateAction()

    async def GetDeleteLineAction(self):
        self.TableData = self.TableData[self.TableData['ColumnSequence'] != self.ColumnSequence]
        counter = 1
        for index, rows in self.TableData.iterrows(): 
            rowline = rows['ColumnSequence']
            self.TableData.loc[self.TableData['ColumnSequence'] == rowline, 'ColumnSequence'] = counter
            counter = counter + 1
        
    # handle alert dialtog close events 

    async def GetAlertMessageClose(self,e):
        print('im closed')
        await self.FormScreen.page.close_dialog_async()

    async def GetAlertMessage(self, Highlight, Commentry):
        self.AlertMessage = AlertDialog(title=Text(Highlight,weight="bold",color="#AD1457"),content=Text(Commentry),
            actions=[TextButton("Yes",on_click=self.GetAlertMessageAction,style=ButtonStyle(shape={"":RoundedRectangleBorder(radius=6)},bgcolor="#AD1457",color="#FAF8F9")),
                     TextButton("No",on_click=self.GetAlertMessageClose,style=ButtonStyle(shape={"":RoundedRectangleBorder(radius=6)},bgcolor="#AD1457",color="#FAF8F9")),],
            actions_alignment=MainAxisAlignment.END,on_dismiss=[])
        await self.FormScreen.page.show_dialog_async(self.AlertMessage)





        