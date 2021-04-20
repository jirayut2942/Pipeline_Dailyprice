from ConnectDB import engine

import pandas as pd
import pandas_datareader as web
import pymysql
from sqlalchemy import create_engine
import sys
import datetime as dt
from datetime import date
from datetime import timedelta
from datetime import datetime
import pytz

# Time zone: thailand
TH = pytz.timezone('Asia/Bangkok')

Set50_list = ['ADVANC.BK','AOT.BK','AWC.BK','BBL.BK','BDMS.BK','BEM.BK','BGRIM.BK','BH.BK','BJC.BK','BPP.BK',
	    'BTS.BK','CBG.BK','CPALL.BK','CPF.BK','CPN.BK','CRC.BK','DTAC.BK','EA.BK','EGCO.BK','GLOBAL.BK','GPSC.BK',
	    'GULF.BK','HMPRO.BK','INTUCH.BK','IRPC.BK','IVL.BK','KBANK.BK','KTB.BK','KTC.BK','LH.BK','MINT.BK','MTC.BK',
	    'OSP.BK','PTT.BK','PTTEP.BK','PTTGC.BK','RATCH.BK','SAWAD.BK','SCB.BK','SCC.BK','TCAP.BK','TISCO.BK','TMB.BK',
	    'TOA.BK','TOP.BK','TRUE.BK','TTW.BK','TU.BK','VGI.BK','WHA.BK']


class pipeline(object):

    def __int__(self):
        self.data_set50= None
        self.data_transfrom= None

    def extract(self):
        try:
            print("Extract-->")
            # find max date in database
            sql = """ select DATE_ADD(max(Date), INTERVAL 1 DAY) as "max(Date)" from set50  """
            Max_Date = pd.read_sql_query(sql, engine)

            # startdate, enddate
            start_time=Max_Date['max(Date)'].loc[0]
            end_time = dt.datetime.now(TH)-dt.timedelta(1)
            end_time = end_time.date()
            self.data_set50 = web.DataReader(Set50_list,'yahoo', start_time, end_time)
            print("Start_date: ",start_time) # Start date stocsk data for Extarct
            print("End_date: ",end_time) # End date stocsk data for Extarct
        except:
	        print(sys.exc_info())



    def transfrom(self):
        try:
            print("Transform-->")
            data = self.data_set50
            ds = data.stack().reset_index()
            titels = list(ds.columns)
            titels[2],titels[3],titels[4],titels[5],titels[6],titels[7] = titels[6],titels[4],titels[5],titels[3],titels[2],titels[7]
            self.data_transfrom = ds[titels]
        except:
	        print(sys.exc_info())



    def load(self):
        try:
            print("Load-->")
            df = self.data_transfrom
            df.to_sql(name='set50', con=engine, if_exists = 'append', index=False )
            print("Process ETL is done")
        except:
	        print(sys.exc_info())


if __name__ == '__main__':
    pipeline=pipeline()
    pipeline.extract()
    pipeline.transfrom()
    pipeline.load()





