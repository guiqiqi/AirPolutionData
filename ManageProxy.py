import subprocess
import time
import threading
import shelve
import queue
import re
from collections import defaultdict
import urllib.request as request
import urllib
import socket

import datetime

socket.setdefaulttimeout(5)

class DBHandler(object):
	"""数据文件操作"""
	def __init__(self, file_name, file_dir):
		self.file_dir = file_dir
		self.file_name = file_name
		self.db = shelve.open(file_dir+"/"+file_name, writeback = True)
	def getAllKey(self, as_queue = True):
		self.ip_list = self.db.keys()
		if not as_queue:
			return self.ip_list
		self.ip_queue = queue.Queue(maxsize = -1)
		for ip in self.ip_list:
			self.ip_queue.put(ip)
		return self.ip_queue
	def getAll(self):
		return dict(self.db.items())
	def get(self, key):
		return self.db.get(key, default = False)
	def update(self):
		self.close()
		self.__init__(self.file_name, self.file_dir)
	def add(self, key, data):
		self.db[key] = data
	def delete(self, key):
		del self.db[key]
	def clear(self):
		for item in self.getAllKey(False):
			self.delete(item)
	def exist(self, key):
		if key in self.db.keys():
			return True
		return False
	def close(self):
		self.db.close()

class TestLag(object):
	"""用于定时测试每个IP的延迟以及可用性"""
	def __init__(self, manager, thread_num = 8, retry_times = 3):
		self.manager = manager
		self.ip_queue = manager.ipfile.getAllKey()
		self.IP_COUNT = self.ip_queue.qsize()
		self.thread_num = thread_num
		self.thread_queue = queue.Queue(maxsize = thread_num)
		self.pattern = re.compile('(?P<gap>\d+)ms')
		self.resultset = {}
		self.retry_times = retry_times
	def start(self):
		ip_queue = self.ip_queue
		for i in range(self.thread_num):
			self.thread_queue.put(i+1)
		while (not ip_queue.empty()) or (len(self.resultset) != self.IP_COUNT):
			try:
				ip = ip_queue.get_nowait()
			except:
				pass
			thread_id = self.thread_queue.get()
			newth = threading.Thread(target = self.gapping, name = 'TestGap-' + str(thread_id),
				args = (ip, thread_id))
			newth.start()
		print (self.resultset)
		ip_queue = self.manager.ipfile.getAllKey()
	def gapping(self, ip, thread_id):
		ret = subprocess.Popen('ping -n 1 %s' % ip,stdout = subprocess.PIPE)
		ret.wait()
		gap = has_gap = self.manager.ipfile.get(ip)
		if (has_gap > 0) and ret.returncode:
			gap = -self.retry_times
		if has_gap < 0:
			gap = has_gap + 1
		if not ret.returncode:
			result = ret.stdout.read()
			t = self.pattern.search(str(result))
			if t:
				gap = int(t.group()[0:-2])
		self.resultset[ip] = gap
		self.thread_queue.put(thread_id)

class ManageAddr(object):
	"""管理相应的IP以及端口，延迟的对应关系"""
	def __init__(self, ipfile_name, portfile_name, file_dir = 'data'):
		self.ipfile = DBHandler(ipfile_name,file_dir)
		self.portfile = DBHandler(portfile_name,file_dir)
	def add(self, ipListWithPort, retry_times = 3):
		for item in ipListWithPort:
			ip_addr = item.split(':')
			ip, port = ip_addr[0], ip_addr[1]
			self.record_ip(ip, retry_times)
			self.record_port(ip, port)
	def get(self, ip):
		return ip+self.portfile.get(ip)
	def record_port(self, ip, port):
		self.portfile.add(ip, port)
	def record_ip(self, ip, retry_times):
		if not self.ipfile.exist(ip):
			self.ipfile.add(ip, -retry_times)
	def getAsGap(self):
		sortasgap = defaultdict(list)
		for k,v in self.ipfile.getAll().items():
			sortasgap[v].append(k)
		try:
			sortasgap = sorted(sortasgap.items(),key = lambda d:d[0])
			ip = sortasgap[0][1][0]
			gap = sortasgap[0][0]
			port = self.portfile.get(ip)
		except:
			return None
		self.delete(ip)
		self.update()
		if gap <= 0:
			return self.getAsGap()
		return [[ip,port],gap]
	def renew(self, has_test):
		for ip in has_test.keys():
			if not has_test[ip]:
				self.ipfile.delete(ip)
				self.portfile.delete(ip)
		self.update()
	def update(self):
		self.ipfile.update()
		self.portfile.update()
	def delete(self, key):
		self.ipfile.delete(key)
		self.portfile.delete(key)
	def close(self):
		self.ipfile.close()
		self.portfile.close()
	def query(self, handler, key = None):
		if key == None:
			return handler.getAllKey()
		return handler.get(key)

class VerifyProxy(object):
	"""通过使用测试代理的可用性"""
	def __init__(self, manager, thread_num):
		self.manager = manager
		self.thread_num = thread_num
		self.ip_queue = manager.ipfile.getAllKey()
		self.deleteList = []
		self.header = [('User-Agent', 'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko'),
		('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'), 
		('Cache-Control', 'no-cache'), ('Connection', 'Keep-Alive'), ('Pragma', 'no-cache'), 
		('Accept-Language', 'zh-CN,zh;q=0.8,en;q=0.6')]
		with open('Verify.data','r') as file:
			self.data = file.read()
	def start(self):
		thread_queue = queue.Queue(maxsize = self.thread_num)
		self.thread_queue = thread_queue
		ip_queue = self.ip_queue
		for i in range(self.thread_num):
			thread_queue.put(i+1)
		while not ip_queue.empty():
			try:
				ip = ip_queue.get_nowait()
				port = self.manager.portfile.get(ip)
			except:
				pass
			thread_id = thread_queue.get()
			newth = threading.Thread(target = self.verifying, 
				name = 'Verify-' + str(thread_id), args = (ip, port, thread_id))
			newth.start()
		for item in self.deleteList:
			self.manager.delete(item)
		self.manager.update()
	def verifying(self, ip, port, thread_id):
		thread_queue = self.thread_queue
		head = self.usingProxy([ip, port])
		thread_queue.put(thread_id)
	def usingProxy(self, address):
		ip = address[0]
		address = ":".join(address)
		proxies = {'http': address}
		proxy_handler = request.ProxyHandler(proxies)
		opener = request.build_opener(request.HTTPHandler,proxy_handler)
		request.install_opener(opener)
		opener.addheaders = self.header
		try:
			response = opener.open("http://www.example.com")
			if response.read().hex() == self.data:
				print (address)
			else:
				self.deleteList.append(ip)
		except:
			self.deleteList.append(ip)

if __name__ == "__main__":
	manager = ManageAddr('IPADDR','PORT','data')
	data = open('data/IP.txt','r').read().split('\n')
	manager.add(data)
	manager.close()
	# verify = VerifyProxy(manager, 16)
	# verify.start()
	# Lapper = TestLag(manager, 8, 3)
	# Lapper.start()
	pass