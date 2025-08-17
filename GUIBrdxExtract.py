
import SYSConnectToServers as CS
import pandas as pd
from flet import *

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
        self.GetColumnSequence(CONID)
        self.GetBrdxTemplate()
        return self.BrdxData, self.BrdxReportTemplate
    
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
        self.GetManualData()
        self.GetFunctionData()

    def GetFunctionData(self):
        if 'Function' in self.RequiredData.keys(): 
            self.FunctionData = self.RequiredData['Function']
            for index, rows in self.FunctionData.iterrows():
                self.BrdxData.loc[self.BrdxData.index, rows['ColumnOutput']] = rows['Variables']
    
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
            # print(LogicData[LogicData['TableName'] == TableName.values[0]]); input()
            self.RequiredData.update({TableName.values[0]:self.BrdxVariables[self.BrdxVariables['TableName'] == TableName.values[0]]}) 
            # print(self.RequiredData); input()
  
        




