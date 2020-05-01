import DataBaseManager
import Functions
import pandas
import os
import threading
import time
import copy
import queue
import random

FileName = "UniHour.csv"

Time = Functions.TimeClass
DBHandler = DataBaseManager.DBHandler("UNIDATABASE")
DF = pandas.read_csv(FileName, encoding = "GBK")
DF = DF.fillna("")
InsertInfo = \
{
	"Index" : {"TIMEPOINT" : "DATETIME"},
	"Others" : {
		"ITEMID" : ("INT", False),
		"VALUE" : ("FLOAT", True),
		"AQI" : ("INT", True),
		"PM2_5" : ("FLOAT", True),
		"PM10" : ("FLOAT", True),
		"CO" : ("FLOAT", True),
		"NO2" : ("FLOAT", True),
		"SO2" : ("FLOAT", True),
		"O3" : ("FLOAT", True),
		"LATITUDE" : ("FLOAT", True),
		"LONGITUDE" : ("FLOAT", True),
		"ETLDATETIME" : ("DATETIME", True),
		"SYNC_DATE" : ("DATETIME", False)
	}
}

class Main(object):
	def __init__(self, **kwargs):
		self.DF = kwargs['DF']
		self.DBHandler = kwargs['DB']
		self.InsertInfo = kwargs['II']
		self.Time = kwargs['T']
		self.BufferSize = kwargs['BS']
		self.MaxWaittingNum = kwargs['MWN']
		self.CreateDaysTable()
		self.InitTaskNum = len(self.DF)
		self.DBHandler.SetStruct(self.InsertInfo)
		self.TableList = self.DBHandler.Tables()
		self.CreateTable()
		self.Current = 0
		self.Did = 0
		self.DoneFlag = False
		self.InsertThreadNum = 0
		self.Buffer = queue.Queue(maxsize = self.BufferSize)
		self.InsertLock = threading.Condition()
		self.ManagerLock = threading.Lock()
		self.NewLock = threading.Lock()
		self.ManagerLock.acquire()
		self.NewLock.acquire()
		self.ManagerThread = threading.Thread(target = self.BufferManager)
		self.ShowRateThread = threading.Thread(target = self.ShowRate)
		self.LimitQueue = queue.Queue(maxsize = self.MaxWaittingNum)

	def CreateTable(self):
		DF = self.DF
		# self.DBHandler.Execute("BEGIN TRANSACTION")
		TableSet = set(DF.AREA.tolist())
		print ("开始创建数据库表...")
		for TableName in TableSet:
			if not TableName in self.TableList:
				self.DBHandler.CreateTable(TableName)
		self.DBHandler.Commit()
		print ("表创建完毕！")

	def InsertLoop(self):
		DF = self.DF
		DBHandler = self.DBHandler
		InsertInfo = self.InsertInfo
		Time = self.Time
		while not self.LimitQueue.full():
			self.LimitQueue.put(1)
		while self.Current < self.InitTaskNum:
			Row = DF.iloc[self.Current]
			Data = Row.to_dict()
			TableName = Data.pop("AREA")
			InsertArrange = list(Data.keys())
			InsertData = list(Data.values())
			TimePoint = Data["TIMEPOINT"]
			if not Time.IsStandardTime(TimePoint):
				InsertArrange.append("AREA")
				InsertData.append(TableName)
				TableName = "DAYS"
			InsertArrange = tuple(InsertArrange)
			InsertData = tuple(InsertData)
			self.Current += 1
			if not self.Buffer.full():
				self.Buffer.put([TableName, InsertData, InsertArrange])
			else:
				self.ManagerLock.release()
				self.NewLock.acquire()
		self.DoneFlag = True
		self.ManagerLock.release() #清理残留数据
		try:
			self.ManagerLock.release()
		except:
			pass
		print ("写入数据......")
		while self.InsertThreadNum != 0:
			time.sleep(1)
		print ("数据库导入任务已完成！")
		print ("写入 %s 次" % str(self.Did))
		return True

	def BufferManager(self):
		while True:
			self.ManagerLock.acquire()
			if self.Buffer.empty():
				break
			TaskQueue = queue.Queue()
			while not self.Buffer.empty():
				TaskQueue.put(self.Buffer.get())
			InsertThread = threading.Thread(target = self.Inserter, args = (TaskQueue,), name = "WT")
			self.InsertThreadNum += 1
			self.LimitQueue.get()
			InsertThread.start()
			self.NewLock.release()

	def Inserter(self, TaskQueue):
		self.LimitQueue.put(1)
		if not self.InsertLock.acquire():
			self.InsertLock.wait()
		DBHandler = DataBaseManager.DBHandler("UNIDATABASE")
		while not TaskQueue.empty():
			Task = TaskQueue.get()
			DBHandler.Insert(Task[0], Task[1], Task[2])
		DBHandler.Commit()
		self.Did += 1
		del DBHandler
		self.InsertThreadNum -= 1
		self.InsertLock.notify()
		self.InsertLock.release()

	def CreateDaysTable(self):
		InsertInfo = copy.deepcopy(self.InsertInfo)
		DBHandler = self.DBHandler
		InsertInfo['index'] = None
		InsertInfo['others']['TIMEPOINT'] = ('DATETIME', False)
		InsertInfo['others']['AREA'] = ('TEXT', False)
		if not DBHandler.HaveTable("DAYS"):
			DBHandler.SetStruct(InsertInfo)
			DBHandler.CreateTable("DAYS")
			return True
		return True

	def ShowRate(self):
		while not self.DoneFlag:
			C = self.Current
			I = self.InitTaskNum
			N = str(round(C / I * 100,2)) + " %"
			print ("已经完成数据库导入任务 " + N)
			time.sleep(15)

	def Run(self):
		self.ManagerThread.start()
		self.ShowRateThread.start()
		time.sleep(1)
		self.InsertLoop()

MainDo = Main(DF = DF, DB = DBHandler, II = InsertInfo, T = Time, BS = 8000, MWN = 50)
MainDo.Run()