import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import codecs
import DataBaseManager
import GetData
from pandas.tools.plotting import andrews_curves
import seaborn as sns

Head = ['AQI', '范围', '质量等级', 'PM2.5', 'PM10', 'SO2', 'CO', 'NO2', 'O3']
CityListO = codecs.open("CityList.info", "r", "utf-8")
CityList = [item.strip('\r') for item in CityListO.read().split('\n')]
CityListO.close()
DataBase = DataBaseManager.DBHandler("DATABASE")
Connection = DataBase.GetConn()
OriginalTime = OT = "20131202"

def queryByDate(startDate, endingDate, city, freq = "D"):
	QueryResult = [list(item) for item in DataBase.Query(city, int(startDate), int(endingDate))]
	index = pd.date_range(startDate, endingDate)
	df = pd.DataFrame(QueryResult)
	Date = df[[0]]
	Data = df[[1,2,3,4,5,6,7,8,9]]
	Data.columns = Head
	ChangeList = ['AQI','PM2.5','PM10','SO2','CO','NO2','O3']
	Data = Data.apply(lambda x: pd.to_numeric(x, errors = 'ignore'))
	Date = pd.to_datetime(Date[0], format='%Y%m%d', errors='ignore')
	Data.index = Date
	Data = Data.resample(freq).mean()
	return Data

Data = queryByDate("20160101", "20161231", "西安", "M")
# Data = Data.T
Data.plot()
sns.set(style="white", color_codes=True)
sns.plt.show()