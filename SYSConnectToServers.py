
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import pandas as pd
import pyodbc
    
# handle ODS server connections, fetch and append data queries 
    
class ConnectToODSServer():

    def __init__(self):

        self.ODSServer = 'FIMVANLT43' #os.getenv("ODSServer")
        self.ODSDatabase = 'ForwardsODS' #os.getenv("ODSDatabase")
        Driver ='ODBC Driver 17 for SQL Server'
        self.ODSConnectionString = f'Driver={Driver};Server={self.ODSServer};Database={self.ODSDatabase};Trusted_Connection=yes;'
        self.ODSConnection = URL.create("mssql+pyodbc", query={"odbc_connect": self.ODSConnectionString})
        self.engineODS = create_engine(self.ODSConnection, use_setinputsizes = False, echo = False)
        self.ODSConnectionPandas = self.engineODS.connect()
        self.ODSConnection = pyodbc.connect(self.ODSConnectionString)

    # try and append data to ODS server 

    def qryODSAppendData(self, query):
        try: return self.ODSConnection.execute(query).commit()
        except Exception as e: print(f"error message {e}"); pass

    # try and extract data from ODS server 
    
    def qryODSGetData(self, query):
        try: return pd.read_sql(query, con=self.ODSConnectionPandas)
        except Exception as e: print(f"error message {e}"); pass
    
# handle ETL server connections, fetch and append data queries 
    
class ConnectToETLServer():

    def __init__(self):

        Driver ='ODBC Driver 17 for SQL Server'
        self.ETLServer = 'FIMVANLT43' #os.getenv("ETLServer")
        self.ETLDatabase = 'ForwardsETL' #os.getenv("ETLDatabase")
        self.ETLConnectionString = f'Driver={Driver};Server={self.ETLServer};Database={self.ETLDatabase};Trusted_Connection=yes;'
        self.ETLConnection = URL.create("mssql+pyodbc", query={"odbc_connect": self.ETLConnectionString})
        self.engineETL = create_engine(self.ETLConnection, use_setinputsizes = False, echo = False)
        self.ETLConnectionPandas = self.engineETL.connect()
        self.ETLConnection = pyodbc.connect(self.ETLConnectionString)

    # try and append data to ETL server 

    def qryETLAppendData(self, query):
        try: return self.ETLConnection.execute(query).commit()
        except Exception as e: print(f"error message {e}"); pass

    # try and extract data from ETL server 
    
    def qryETLGetData(self, query):
        try: return pd.read_sql(query, con=self.ETLConnectionPandas)
        except Exception as e: print(f"error message {e}"); pass
    
# handle ODS server connections, load data queries 
    
class LoadDataToODS():

    def __init__(self):
        self.ETL = ConnectToETLServer()
        self.ODS = ConnectToODSServer()
        self.CON = ConnectToCONServer()
        self.Test = ConnectToTestServer()

    def LoadDataToODS(self, Data, TableName, DB):

        DateList  = ['DATE','SaleDate','QuoteDate','CreatedOn','TransactionDate','TimeStamp','AccountEffectiveDate','InvoiceDate']
        ErrList = ['nan', 'None', 'NaT']
        for index, Lines in Data.iterrows():
            qrySelect = ''; qryInsert = ''
            for ColumnName in Lines.index: 
                RowData =  Lines[ColumnName]; qryInsert = qryInsert+'['+ColumnName+'], '  
                RowData = str(RowData).replace("'","")   
                if RowData in ErrList: RowData = ''
                if ColumnName in DateList: RowData = str(RowData)[0:19] 
                qrySelect = qrySelect + "'" + str(RowData) + "' AS " + ColumnName +', ' 
            qrySelect1  = qrySelect[0:(len(qrySelect)-2)]; qryInsert1  = qryInsert[0:(len(qryInsert)-2)]
            qryLoadData = f'INSERT INTO {TableName} (' + qryInsert1 + ') Select ' + qrySelect1 
            # print(qryLoadData);input()
            if DB == 'ETL': Cursor = self.ETL.ETLConnection.cursor()
            elif DB == 'ODS': Cursor = self.ODS.ODSConnection.cursor()
            elif DB == 'CON': Cursor = self.CON.CONConnection.cursor()
            elif DB == 'Test': Cursor = self.Test.TestConnection.cursor()
            Cursor.execute(qryLoadData); Cursor.commit()

# handle Testing server connections, fetch and append data queries 
    
class ConnectToTestServer():

    def __init__(self):

        Driver ='ODBC Driver 17 for SQL Server'
        self.TestServer = 'FIMVANLT43' 
        self.TestDatabase = 'Testing' 
        self.TestConnectionString = f'Driver={Driver};Server={self.TestServer};Database={self.TestDatabase};Trusted_Connection=yes;'
        self.TestConnection = URL.create("mssql+pyodbc", query={"odbc_connect": self.TestConnectionString})
        self.engineTest = create_engine(self.TestConnection, use_setinputsizes = False, echo = False)
        self.TestConnectionPandas = self.engineTest.connect()
        self.TestConnection = pyodbc.connect(self.TestConnectionString)

    # try and append data to Test server 

    def qryTestAppendData(self, query):
        try: return self.TestConnection.execute(query).commit()
        except Exception as e: print(f"error message {e}"); pass

    # try and extract data from Test server 
    
    def qryTestGetData(self, query):
        try: return pd.read_sql(query, con=self.TestConnectionPandas)
        except Exception as e: print(f"error message {e}"); pass

# handle CON server connections, fetch and append data queries 
    
class ConnectToCONServer():

    def __init__(self):

        Driver ='ODBC Driver 17 for SQL Server'
        self.CONServer = 'FIMVANLT43' 
        self.CONDatabase = 'ConsolidatedDB'
        self.CONConnectionString = f'Driver={Driver};Server={self.CONServer};Database={self.CONDatabase};Trusted_Connection=yes;'
        self.CONConnection = URL.create("mssql+pyodbc", query={"odbc_connect": self.CONConnectionString})
        self.engineCON = create_engine(self.CONConnection, use_setinputsizes = False, echo = False)
        self.CONConnectionPandas = self.engineCON.connect()
        self.CONConnection = pyodbc.connect(self.CONConnectionString)

    # try and append data to ETL server 

    def qryCONAppendData(self, query):
        try: return self.CONConnection.execute(query).commit()
        except Exception as e: print(f"error message {e}"); pass

    # try and extract data from ETL server 
    
    def qryCONGetData(self, query):
        try: return pd.read_sql(query, con=self.CONConnectionPandas)
        except Exception as e: print(f"error message {e}"); pass