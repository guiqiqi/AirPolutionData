import datetime

class TimeClass(object):
	def GetTimestamp(strtime):
		try:
			return int(time.mktime(timetuple))
		except ValueError:
			return False

	def GetTimestr(time_stamp):
		model = '%Y/%m/%d %H:%M'
		time_tuple = time.localtime(int(time_stamp))
		return time.strftime(model, time_tuple)

	def GetStandardTime(strtime):
		model = '%Y/%m/%d %H:%M'
		time = datetime.datetime.strptime(strtime, model)
		time = time.strftime(model)
		return time

	def IsStandardTime(strtime):
		model = '%Y/%m/%d %H:%M'
		try:
			time = datetime.datetime.strptime(strtime, model)
		except ValueError:
			return False
		return True