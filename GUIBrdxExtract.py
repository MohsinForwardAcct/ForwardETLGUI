
import SYSConnectToServers as CS
import pandas as pd
from flet import *
import numpy as np

class DownloadBrdxReport():

    def __init__(self):
        
        self.ODS = CS.ConnectToODSServer()
        self.ETL = CS.ConnectToETLServer()

    # generate brdx report 

    async def GetBrdxDownloadData(self, ReportingYear, ReportingPeriod, CONID, PremiumCategory, ProductCode, ContractNumber):   
        query = f"Select * from RESVBrdxReportVariables where CONID = '{CONID}' and ProductCode = '{ProductCode}' and Status = 'Activated'"
        self.BrdxVariables = pd.DataFrame(self.ODS.qryODSGetData(query))
        self.ODS.ODSConnection.close
        self.TableNames = self.BrdxVariables[['TableName']].drop_duplicates('TableName', keep='first') 
        self.GetRequiredData()
        self.GetTableData(ReportingPeriod, CONID, PremiumCategory, ProductCode, ContractNumber)
        self.GetBrdxData()
        self.GetDateFixes()
        self.GetColumnSequence(CONID)
        self.GetBrdxDataClean()
        self.GetBrdxTemplate()
        return self.BrdxData, self.BrdxReportTemplate
    
    def GetBrdxDataClean(self):
        FuncSupport = [col for col in self.BrdxData.columns if 'FuncSupport' in col]
        # print(FuncSupport); input()
        self.BrdxData = self.BrdxData.drop(columns=FuncSupport)

    def GetDateFixes(self):
        DateFixes = self.BrdxVariables[self.BrdxVariables['Variables'].isin(['EffectiveChange_DATE','Cancellation_DATE'])]['ColumnOutput']
        for Col in DateFixes: self.BrdxData.loc[self.BrdxData[Col] == '1900-01-01 00:00:00', Col] = ''

    def GetBrdxTemplate(self):
        self.BrdxReportTemplate = self.BrdxData.head(1).reset_index(drop=True)
        self.BrdxReportTemplate.loc[self.BrdxReportTemplate.index,'Template'] = 'Template'
    
    def GetColumnSequence(self, CONID):
        ExemptList = ['JETALISStatus', 'JETALISMatchType', 'ALISInvoiceNumber', 'ALISInvoiceDate', 'ALISPremium', 'ALISAccountEffectDate', 'Notes']
        self.BrdxTemplate = pd.DataFrame(self.ODS.qryODSGetData(f"Select * from RESVBrdxReportTemplates where CONID = '{CONID}' order by ColumnSequence"))
        self.BrdxTemplate.sort_values('ColumnSequence')      
        ColumnSequence = pd.Series(self.BrdxTemplate['ColumnOutput'])
        ColumnSequence = ColumnSequence[~ColumnSequence.isin(ExemptList)]
        self.BrdxData = self.BrdxData[ColumnSequence]

    def GetBrdxData(self):
        self.BrdxData = self.TableData['FACTData']
        for TableName in self.TableData.keys():
            if TableName != 'FACTData': self.BrdxData = self.BrdxData.merge(self.TableData[TableName], how='left', on='UID')
        # print(self.BrdxData); self.BrdxData.to_clipboard(); input()
        self.GetManualData()
        self.GetFunctionData()

    def GetFunctionData(self):
        if 'Function' in self.RequiredData.keys(): 
            self.FunctionData = self.RequiredData['Function']
            for index, rows in self.FunctionData.iterrows():
                self.AppendColumn = rows['ColumnOutput']; self.FormulaData = rows['Variables'].split("|")
                self.GetFuncVariablesLogic()
                FuncVariablesDataRow = self.BrdxVariables[self.BrdxVariables['Variables'].isin(self.Variables)]
                FuncVariablesDataRow = FuncVariablesDataRow.drop_duplicates('Variables', keep='first')
                self.FuncVariablesColumnOutput = FuncVariablesDataRow['ColumnOutput'].values
                self.FuncVariablesColumnVariables = FuncVariablesDataRow['Variables'].values
                self.GetFuncEditDataFrame()    

    def GetFuncEditDataFrame(self):
        for index, rows in self.BrdxData.iterrows():
            UID = rows['UID']; params = {}; counter = 0
            RowData = self.BrdxData[self.BrdxData['UID'] == UID]
            for col in self.FuncVariablesColumnOutput: 
                VariablesValue = RowData[col].values[0]
                if isinstance(VariablesValue, np.float64): VariablesValue = float(VariablesValue)
                params.update({self.FuncVariablesColumnVariables[counter]:VariablesValue}); counter = counter + 1 
            params.update({'formula':self.Logic})
            # print(UID, params); input()
            result = eval(params["formula"],  params)
            self.BrdxData.loc[self.BrdxData['UID'] == UID, self.AppendColumn] = result         

    def GetFuncVariablesLogic(self):
        self.Variables = []
        # print(self.FormulaData); input()
        items = self.FormulaData[0].split(","); 
        for item in items: self.Variables.append(item.replace("[","").replace("]","").strip())
        self.Logic = self.FormulaData[1]

    def GetManualData(self):
        self.ManualData = self.RequiredData['DIMManualData']
        for index, rows in self.ManualData.iterrows():
            self.BrdxData.loc[self.BrdxData.index, rows['ColumnOutput']] = rows['Variables']

    def GetTableData(self, ReportingPeriod, CONID, PremiumCategory, ProductCode, ContractNumber):   
        self.TableData = {} 
        for TableName in self.RequiredData.keys():
            if TableName != 'DIMManualData' and TableName != 'Function':
                TempTableData = self.RequiredData[TableName]; qrySelect = ''
                for index, rows in TempTableData.iterrows(): 
                    Variables = rows['Variables']; ColumnOutput = rows['ColumnOutput']
                    qrySelect = f"{qrySelect} {str(Variables)} as [{str(ColumnOutput)}]," 
                qrySelect = qrySelect[0:(len(qrySelect)-1)]
                if TableName == 'FACTData': 
                    qrySelect = f"Select {qrySelect} from {TableName} where ReportingPeriod='{ReportingPeriod}' AND CONID='{CONID}' \
                    AND ProductCode='{ProductCode}' AND ContractNumber='{ContractNumber}' AND PremiumCategory='{PremiumCategory}'"
                else: qrySelect = f"Select UID, {qrySelect} from {TableName} where ReportingPeriod ='{ReportingPeriod}' AND CONID='{CONID}' AND ProductCode='{ProductCode}'"
                TempTableData = pd.DataFrame(self.ETL.qryETLGetData(qrySelect)); self.TableData.update({TableName:TempTableData}) 
        
    def GetRequiredData(self):    
        self.RequiredData = {} 
        for index, TableName in self.TableNames.iterrows():
            self.RequiredData.update({TableName.values[0]:self.BrdxVariables[self.BrdxVariables['TableName'] == TableName.values[0]]}) 
  
        




