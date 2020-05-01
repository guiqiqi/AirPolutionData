import requests
import threading
import queue
import bs4
import time
import codecs
import DataBaseManager
import winsound

cityListO = codecs.open("CityList.info","r","utf-8")
cityList =  [item.strip('\r') for item in cityListO.read().split('\n')]
cityListO.close()
DataBase = DataBaseManager.DBHandler("DATABASE")

class GetData(object):
	def __init__(self):
		self.__root__ = "https://www.aqistudy.cn"
		self.__source__ = "historydata"
		self.monthFile = "monthdata.php"
		self.dayFile = "daydata.php"

	def Contenter(target_link, params = None, timeout = 15):
		header = {
		'Accept': 'text/html,application/xml;q=0.9,*/*;q=0.8',
		'Accept-Encoding': 'deflate, gzip',
		'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6',
		'Cache-Control': 'no-cache',
		'Connection': 'Keep-Alive','Pragma':'no-cache',
		'Content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
		'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
		AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'}
		try:
			data = requests.get(url = target_link, params = params,
				timeout = timeout, headers = header)
		except:
			return (target_link, params, False)
		content = data.text
		return (target_link, params, content)

	def LinkParser(self, city_name, suffix = ""):
		params = {'city': city_name} if not suffix else None
		root = self.__root__
		source = self.__source__ if not suffix else suffix
		month = "monthdata.php" if not suffix else ""
		link = ("/").join((root, source, month))
		return (link, params)

	def CompletUrl(self, urlList):
		root = self.__root__
		source = self.__source__
		completed = []
		for link in urlList:
			new = ("/").join((root, source, link))
			completed.append(new)
		return completed

class AnalysisData(object):
	def __init__(self, text):
		if not text:
			raise ValueError("Interet Connection Error!")
		soup = self.soup = bs4.BeautifulSoup(text, "html.parser")

	def find(self, tag_name, attr = ""):
		soup = self.soup
		resultList = []
		found = soup.findAll(tag_name, attrs = attr)
		resultSet = self.warrper(found)
		for item in resultSet:
			if self.colation(item):
				resultList.append(item)
			continue
		return resultList

	def fetchUrl(self, ResultSet = None):
		soup = self.soup
		UrlList = []
		if not ResultSet:
			rs = soup.findAll("a")
		else:
			rs = ResultSet
		for cs in rs:
			for link in cs.findAll("a"):
				UrlList.append(link.get('href'))
		return UrlList

	def head(self):
		soup = self.soup
		return soup.find('tr').getText().strip('\n').split('\n')

	def colation(self, result):
		return True

	def warrper(self, result):
		return result

class PageContent(AnalysisData):
	def colation(self, Result):
		if len(Result) != self.headLength:
			return False
		return True

	def warrper(self, ResultSet):
		rst = [item.getText() for item in ResultSet] # 把HTML标签去掉
		del rst[0] # 删除head部分
		rs = [item.strip('\n').split('\n') for item in rst] # 去除换行组成数组
		return rs

class initial(object):
	def GetPageData(link):
		content = GetData.Contenter(link)[2]
		PageAnalyser = PageContent(content)
		headLength = len(PageAnalyser.head())
		PageAnalyser.headLength = headLength
		resultList = PageAnalyser.find('tr')
		for item in resultList:
			item[0] = int(''.join(item[0].split('-')))
		return resultList

	def GetDownUrl(cityName):
		getData = GetData()
		target_link = getData.LinkParser(cityName)
		content = GetData.Contenter(target_link[0], target_link[1])[2]
		analysis = AnalysisData(content)
		resultList = analysis.find('tr')
		linkList = analysis.fetchUrl(resultList)
		urlList = getData.CompletUrl(linkList)
		return (cityName, urlList)

	def GetHead():
		random_city = cityList[0]
		getData = GetData()
		link = getData.LinkParser(random_city)
		get = GetData.Contenter(link[0], link[1])[2]
		anayHead = AnalysisData(get)
		head = anayHead.head()
		return head

	def CreateTable():
		head = initial.GetHead()
		head[0] = "时间"
		DataBase.setStruct(head)
		for city in cityList:
			DataBase.CreateTable(city)

	def insertRecord(city, getList):
		for item in getList:
			DataBase.insert(city, item)

class Spider(object):
	def __init__(self, taskUrl, maxThreadNum = 6):
		self.maxThreadNum = maxThreadNum
		self.lockThreadQueue = queue.Queue(maxsize = maxThreadNum)
		while not self.lockThreadQueue.full():
			self.lockThreadQueue.put(1)
		self.taskUrl = taskUrl
		self.Done = []

	def getJob(self):
		taskUrl = self.taskUrl
		try:
			Job = taskUrl.pop()
		except IndexError:
			return None
		return Job

	def JobIsEmpty(self):
		if len(self.taskUrl) == 0:
			return True
		return False

	def move(self, Job):
		try:
			Data = initial.GetPageData(Job)
			try:
				assert len(Data) != 0
			except:
				self.lockThreadQueue.put(1)
				print ("空任务 : " + Job)
				return 0
		except ValueError:
			print ("重试 : " + Job)
			self.move(Job)
		else:
			self.Done += Data
			self.lockThreadQueue.put(1)

	def go(self):
		while not self.JobIsEmpty():
			self.lockThreadQueue.get()
			CurrentJob = self.getJob()
			leg = threading.Thread(target = self.move, name = "WeatherSpider", args = (CurrentJob,))
			leg.setDaemon(True)
			leg.start()
		print ("等待回收数据")
		while not self.lockThreadQueue.full():
			time.sleep(1)
		return self.Done

def main():
	for city in cityList:
		print ("处理的城市 ： " + str(city))
		try:
			taskList = initial.GetDownUrl(city)[1]
		except:
			for i in range(6):
				winsound.Beep(1000,1000)
				print ("主程序错误！")
		print ("任务数量 ： " + str(len(taskList)))
		Spider_test = Spider(taskList, 4)
		get = Spider_test.go()
		print ("写入数据 " + str(len(get)) + " 条......")
		initial.insertRecord(city, get)
		print ("主程序休眠中..........")
		time.sleep(2)
	print ("所有任务完成!")

if __name__ == '__main__':
	del DataBase
