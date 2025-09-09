
from flet import *
import SYSConnectToServers as CS
import pandas as pd
import GUILogicTables as LT
import GUIBrdxTemplates as BrdxTemplate
import GUIBrdxVariables as BrdxVariables
import GUIFormAdd as GFA
import GUIFormUpdate as GFU
import GUIBrdxTemplatesEdit as GTE
import GUIBrdxVariablesEdit as GVE
import main as mn


class FormScreen(Container):

    def __init__(self, page, download, username, Query, ScreenName, ReportTitle):
        super().__init__()

        self.page = page
        self.Download = download
        self.UserName = username
        self.ODS = CS.ConnectToODSServer()
        self.ColumnNames = []; self.RowsData = []; self.RowValues = ''
        self.Query = Query; self.ScreenName = ScreenName; self.ReportTitle = ReportTitle; self.ColumnSelect = ScreenName
        self.TableList = pd.DataFrame(self.ODS.qryODSGetData('select distinct TableName from RESVVariablesLogic')); self.ODS.ODSConnection.close
        self.VariableList = pd.DataFrame(self.ODS.qryODSGetData('select distinct ColumnName from RESVVariablesLogic')); self.ODS.ODSConnection.close
        self.TableData = pd.DataFrame(self.ODS.qryODSGetData(self.Query)); self.ODS.ODSConnection.close
        self.GetRefineTableData()
        self.FormScreen = Column(expand=True, controls=[])
        self.CurrentPage = 1; self.TotalPages = (self.TableData.shape[0] + 10 - 1) // 10
        self.TableName = None

    # filter table columns

    def GetRefineTableData(self):
        BrdxVariablesList = ['UID', 'BrdxVariableID', 'BrdxTemplateID']
        BrdxTemplateList = ['UID', 'BrdxTemplateID']
        if self.ScreenName == 'BrdxVariablesScreen': self.TableData = self.TableData.drop(BrdxVariablesList, axis=1)
        elif self.ScreenName == 'BrdxTemplates': self.TableData = self.TableData.drop(BrdxTemplateList, axis=1)
        # print(self.TableData); input()

    # combine form header, fields, table and pagination

    def GetFormScreen(self):
        self.GetFormHeader(); self.GetFormFields(); self.GetFormTable(); self.GetPagination()
        self.ReportScreen = Column(expand=True,controls=[Row(expand = False, controls=[self.FormHeader]), 
                                Row(expand = False, controls=[self.FormFields]),self.FormTable, self.Pagination])
        self.FormScreen.controls.append(self.ReportScreen)
        return self.FormScreen
    
    # create form header
    
    def GetFormHeader(self):
        self.FormHeader = Container(expand=True, height=50, bgcolor="#AD1457",border_radius=border_radius.only(top_left=15,top_right=15),
                        padding=padding.only(left=15,right=15),content=Row(expand=True,alignment=MainAxisAlignment.SPACE_BETWEEN,
                        controls=[IconButton(icon=icons.NAVIGATE_BEFORE_ROUNDED,icon_color=colors.WHITE, on_click = self.GetNavigateBack),
                            Container(expand=True,content=Text(self.ReportTitle,text_align="left",size=18,color="white",weight='bold'))]))   

    # create form table 
    
    def GetFormTable(self):
        for cols in self.TableData.columns.values: self.ColumnNames.append(DataColumn(Text(cols,size=15,color='#AD1457',weight='bold')))
        Start = (self.CurrentPage-1); End = Start + 10
        for index, rows in self.TableData[Start:End].iterrows():
            self.RowsData.append(DataRow(cells=[DataCell(Text(value, color="white",size=13))for value in rows.values],on_select_changed = self.PageLogicTables))  
        self.FormTableData = DataTable(expand=True, border=border.all(2,"#ebebeb"), border_radius=10, columns=self.ColumnNames, rows=self.RowsData)
        self.FormTable =  Container(border_radius=10, col = 8, content=Column(expand=True, scroll='auto', controls=[ResponsiveRow([self.FormTableData])]))

    # create form fields

    def GetFormFields(self):
        self.GetFormSwitches()
        self.FormFields = Container(expand=True,height=150,bgcolor="white10",border=border.all(1,"#ebebeb"),border_radius=8,padding=15,
                        content=Column(expand=True,controls=[Row(expand=True,controls=self.GetFormFieldsBuild()),
                        Divider(height=2,color="transparent"),Row(expand=True,alignment=MainAxisAlignment.END,controls=[self.GetSwitchesBuild(self.Switches)])]))
        
    # on click logics
        
    async def PageLogicTables(self,e):
        counter = 0
        if self.ScreenName == 'LogicTable': valuecounter = 2
        else: valuecounter = 3
        self.TableNameValue = e.control.cells[valuecounter].content.value; self.ColumnSequence = e.control.cells[0].content.value 
        for Value in self.FormFields.content.controls[0].controls:
            self.ObjectType = str(self.FormFields.content.controls[0].controls[counter].content.controls[0])
            self.FieldName = self.FormFields.content.controls[0].controls[counter].content.controls[0].value
            self.FieldData = e.control.cells[counter].content.value
            # print(self.ObjectType, self.FieldName, self.FieldData)
            if self.ObjectType[:4] == 'text': 
                # if self.FieldName == 'TableName' and self.FieldData == '': print('this is test')
                self.FormFields.content.controls[0].controls[counter].content.controls[1].value = self.FieldData
            counter = counter + 1
        await self.FormFields.update_async()

    # build form fields

    def GetFormFieldsBuild(self):
        TableFrameColumns = self.TableData.columns; ColumnNames = []; counter = 0; self.Ability = 'True'
        for value in TableFrameColumns: 
            self.FieldValue = value
            ColumnNames.append(self.GetFieldsBuild())
            counter = counter + 1
        return ColumnNames
    
    # append form fields

    def GetFieldsBuild(self):
        print(self.FieldValue, self.TableName, self.ScreenName)
        if self.FieldValue == 'TableName' and self.ScreenName == 'SelectedProductScreen': FieldData = self.GetFieldDropdownValue()
        elif self.FieldValue == 'VariableName' and self.TableName != None and self.ScreenName == 'SelectedProductScreen': FieldData = self.GetFieldDropdownValue()
        else: FieldData = self.GetFieldTextValue()
        return FieldData

    # get form field text value

    def GetFieldTextValue(self):
        FieldData = Container(expand=2,height=45,bgcolor="#ebebeb",border_radius=6,padding=8,content=Column(spacing=1,controls=[
                Text(value=f'{self.FieldValue}',size=9,color="black",weight="bold"),TextField(border_color="transparent",height=20,text_size=13,disabled=self.Ability,
                content_padding=0,cursor_color="black",cursor_width=1,cursor_height=18,color="black")]))
        return FieldData
    
    # get form field dropdown value

    def GetFieldDropdownValue(self):
        FieldData = Container(expand=2,height=45,bgcolor="#ebebeb",border_radius=6,padding=8,content=Column(spacing=1,controls=[
                Dropdown(on_change=self.Dropdownvalue,label=f'{self.FieldValue}',color="white",border_color="transparent",height=20,text_size=13,
                options=self.GetDropDownOptions())]))
        return FieldData
    
    # get dropdown options
    
    def GetDropDownOptions(self):
        # print(self.FieldValue, self.TableName)
        self.ProductCode = str(self.ReportTitle).split("-")[1].strip()
        if self.FieldValue == 'TableName': 
            self.OptionQuery = f"select distinct TableName from RESVVariablesLogic where {self.ProductCode} = 'Activate'"
        elif self.FieldValue == 'VariableName' and self.TableName != None: 
            self.OptionQuery = f"select distinct ColumnName from RESVVariablesLogic where {self.ProductCode} = 'Activate' and TableName = '{self.TableName}'"
        self.OptionsData = pd.DataFrame(self.ODS.qryODSGetData(self.OptionQuery)).values; self.ODS.ODSConnection.close
        options = []
        for self.FieldValue in self.OptionsData:
            options.append(dropdown.Option(self.FieldValue[0]))
        return options
    
    # get screen change 

    async def Dropdownvalue(self,e):
        # print(e.control.value, self.ColumnSequence, self.FieldValue, self.TableName)
        if e.control.value in self.TableList.values: 
            self.TableData.loc[self.TableData['ColumnSequence'] == self.ColumnSequence, 'TableName'] = e.control.value
            self.TableName = e.control.value
            await self.GetScreenChange()
        elif e.control.value in self.VariableList.values: 
            self.TableData.loc[self.TableData['ColumnSequence'] == self.ColumnSequence, 'VariableName'] = e.control.value
            self.VariableName = e.control.value
            await self.GetScreenChange()

    # get the action buttons build
            
    def GetSwitchesBuild(self, Switches):
        self.BuildSwitches = Row(controls=[])
        for switch in Switches:
            if switch[0] == 'Edit Mode': self.BuildSwitches.controls.append(Switch(label=switch[0],on_change=switch[2],
            active_color="#AD1457",focus_color="#AD1457",label_position=LabelPosition.LEFT))
            else: 
                if switch[0] == 'Download': mn.TableData = self.TableData; mn.ReportName = self.ReportTitle
                self.BuildSwitches.controls.append(ElevatedButton(on_click=switch[2],bgcolor="#AD1457",color="white",content=Row(controls=[Icon(name=switch[1],size=15, 
            color=colors.WHITE),Text(switch[0], size=12, weight="bold")]),style=ButtonStyle(shape={"":RoundedRectangleBorder(radius=6)},color={"":"white"}),height=switch[3],
            width=switch[4],disabled=switch[5]))
        return self.BuildSwitches
    
    
    def SelectFiles(self, e: FilePickerResultEvent):
        if e.files is not None: print( e.files)
    
    # create option for action buttons

    def GetFormSwitches(self):
        self.Switches = []
        AddSwitch = ['Add New', 'Add_Rounded', self.GetAddSwitch,30,120,'False']
        UpdateSwitch = ['Update', 'Update', self.GetUpdateSwitch,30,120,'False']
        DeleteSwitch = ['Delete', 'Delete', self.GetDeleteSwitch,30,120,'True']
        ValidateSwitch = ['Validate', 'Done_Rounded', self.GetValidateSwitch,30,120,'False']
        UploadSwitch = ['Upload', 'Upload_Rounded', self.GetUploadSwitch,30,120,'True']
        DownloadSwitch = ['Download','download_done_rounded',self.Download,30,120,'False']
        EditSwitch = ['Edit Mode', 'EDIT', self.GetEditSwitch,30,120,'False']
        Remapping = ['ReMapping', 'EDIT', self.GetRemapping,30,120,'False']
        # print(self.ScreenName); input()
        if self.ScreenName == 'SelectedProductScreen': self.Switches = [ValidateSwitch, UploadSwitch, DownloadSwitch]
        elif self.ScreenName == 'BrdxTemplates': self.Switches = [DownloadSwitch, EditSwitch]
        elif self.ScreenName == 'VariableList': self.Switches = [AddSwitch, UpdateSwitch, DownloadSwitch]
        elif self.ScreenName == 'BrdxVariablesScreen': self.Switches = [DownloadSwitch, EditSwitch]
        else: self.Switches = [DownloadSwitch]

    # Edit button action

    async def GetEditSwitch(self, e):
        if self.ScreenName == 'BrdxTemplates': self.ReportScreen = GTE.FormEditScreen(self.page, self.Download, self.UserName, self.Query, self.ScreenName, self.ReportTitle).GetFormEditScreen()
        elif self.ScreenName == 'BrdxVariablesScreen': self.ReportScreen = GVE.FormEditScreen(self.page, self.Download, self.UserName, self.Query, self.ScreenName, self.ReportTitle).GetFormEditScreen()
        self.FormScreen.controls.clear()
        self.FormScreen.controls.append(self.ReportScreen)
        await self.FormScreen.update_async()

    async def GetRemapping(self, e):
        DataValue = self.ReportTitle.split('-'); self.CONID = DataValue[0].strip(); 
        self.ScreenName = 'SelectedProductScreen'
        self.PremiumCategory = self.TableData.head(1)['PremiumCategory'].values[0]
        print(self.CONID, self.PremiumCategory)
        query = f"Select ColumnOutput from RESVBrdxReportTemplates where CONID = '{self.CONID}' and PremiumCategory = '{self.PremiumCategory}'"
        self.TemplateData = pd.DataFrame(self.ODS.qryODSGetData(query)); self.ODS.ODSConnection.close
        for value in self.TemplateData.values:
            if value not in self.TableData['ColumnOutput'].values:
                print(value[0])


    # delete button action

    def GetDeleteSwitch(self, e):
        pass

    # upload button action

    def GetUploadSwitch(self, e):
        pass

    # validation button action
    
    async def GetValidateSwitch(self, e):
        EmptyTableName = self.TableData[self.TableData['TableName'] == ''].shape[0]
        EmptyVariableName = self.TableData[self.TableData['VariableName'] == ''].shape[0]
        if EmptyTableName > 0 or EmptyVariableName > 0: 
            self.GetAlertMessage()
            await self.FormScreen.page.show_dialog_async(self.AlertMessage)
        else: self.FormFields.content.controls[2].controls[0].controls[1].disabled = 'False'
        await self.FormFields.update_async()

    def GetAlertMessage(self):
        self.AlertMessage = AlertDialog(title=Text("Validation Failed",weight="bold",color="#AD1457"),
                                        content=Text("TableName & VariableName Cannot be blank!"),
                actions=[TextButton("Ok",on_click=self.GetAlertMessageClose,style=ButtonStyle(shape={"":RoundedRectangleBorder(radius=6)},
                    bgcolor="#AD1457",color="white"))],actions_alignment=MainAxisAlignment.END,on_dismiss=[])

    async def GetAlertMessageAction(self,e):
        print('Im open')
        await self.FormScreen.page.close_dialog_async()

    async def GetAlertMessageClose(self,e):
        print('im closed')
        await self.FormScreen.page.close_dialog_async()


    async def GetNavigateBack(self, e):
        print(self.ScreenName)
        if self.ScreenName == 'LogicTable': self.ReportScreen = LT.LogicTables(self.page, self.Download, self.UserName).ReportingScreen
        elif self.ScreenName == 'BrdxTemplates': self.ReportScreen = BrdxTemplate.BrdxTemplates(self.page, self.Download, self.UserName).ReportingScreen
        elif self.ScreenName == 'BrdxVariablesScreen' or self.ScreenName == 'SelectedProductScreen': 
            self.ReportScreen = BrdxVariables.BrdxVariables(self.page, self.Download, self.UserName).ReportingScreen
        self.FormScreen.controls.clear()
        self.FormScreen.controls.append(self.ReportScreen)
        await self.FormScreen.update_async()

    async def GetScreenChange(self):
        self.ColumnNames.clear(); self.RowsData.clear()
        self.GetFormScreen()
        self.FormScreen.controls.clear()
        self.FormScreen.controls.append(self.ReportScreen)
        await self.FormScreen.update_async()

    async def GetAddSwitch(self, e):
        self.ReportScreen = GFA.FormAddScreen(self.page, self.Download, self.Query, self.ScreenName, self.ReportTitle).GetFormAddScreen()
        self.FormScreen.controls.clear()
        self.FormScreen.controls.append(self.ReportScreen)
        await self.FormScreen.update_async()
    
    async def GetUpdateSwitch(self, e):
        UpdateValue = str(self.FormFields.content.controls[0].controls[0].content.controls[1].value)
        print(len(UpdateValue))
        if len(UpdateValue) > 0:
            self.ReportScreen = GFU.FormUpdateScreen(self.page, self.Download, self.Query, self.ScreenName, self.ReportTitle,self.FormFields).GetFormUpdateScreen()
            self.FormScreen.controls.clear()
            self.FormScreen.controls.append(self.ReportScreen)
            await self.FormScreen.update_async()
        else: pass

    def GetPagination(self):
        PreviousButtonSwitches = [['','NAVIGATE_BEFORE_OUTLINED',self.GetPreviousPage,20,50,'False']]
        PreviousButton = self.GetSwitchesBuild(PreviousButtonSwitches)
        NextButtonSwitches = [['','NAVIGATE_NEXT_OUTLINED',self.GetNextPage,20,50,'False']]
        NextButton = self.GetSwitchesBuild(NextButtonSwitches)
        PageNum = (self.CurrentPage//10) + 1
        PageLabel = Text(f"Page {PageNum} of {self.TotalPages}",color="white")
        self.Pagination = Container(content=Row([PreviousButton, PageLabel, NextButton], alignment=MainAxisAlignment.CENTER))
   
    async def GetNextPage(self,e):
        if self.CurrentPage > 0 and ((self.CurrentPage+10)/10) < self.TotalPages:
            self.CurrentPage = self.CurrentPage + 10
            await self.GetScreenChange()

    async def GetPreviousPage(self,e):
        if self.CurrentPage > 10 and (self.CurrentPage/10) < self.TotalPages:
            self.CurrentPage = self.CurrentPage - 10
            await self.GetScreenChange()

