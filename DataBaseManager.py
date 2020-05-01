import sqlite3
import copy

class DBHandler(object):
	def __init__(self, FileName, InMem = False):
		self.File = FileName if not InMem else ":memory:"
		self.Database = sqlite3.connect(self.File)

	def __YieldCommand(self, SubKeyInfo):
		Keys = SubKeyInfo.keys()
		for Key in Keys:
			Last = ""
			Info = SubKeyInfo[Key]
			Type = Info[0].upper()
			IsNull = "" if Info[1] else "NOT NULL"
			ItemList = [Key, Type, IsNull]
			Complete = " ".join(ItemList).rstrip(" ") + ","
			yield Complete + Last

	def setStruct(self, Struct):
		self.Struct = Struct
		self.IndexInfo = Struct['index']
		self.SubKeyInfo = Struct['others']
		return True

	def createTable(self, TableName):
		Database = self.Database
		Head = "CREATE TABLE %s" % TableName
		SubKeyInfo = copy.deepcopy(self.SubKeyInfo)
		CommandIter = self.__YieldCommand(SubKeyInfo)
		IndexInfo = self.IndexInfo
		Last = ""
		if IndexInfo:
			Index = set(IndexInfo.keys()).pop()
			IndexType = IndexInfo[Index].upper()
			First = "(" + Index + " %s PRIMARY KEY NOT NULL," % IndexType
		else:
			First = "("
		try:
			while True:
				Last += CommandIter.__next__()
		except StopIteration:
			Command = First + Last.rstrip(',') + ");"
			Command = Head + Command
		self.Execute(Command)
		return Command

	def execute(self, Command):
		Database = self.Database
		Get = Database.execute(Command)
		# Database.commit()
		return Get

	def insert(self, TableName, InsertValue, InsertSeq = None):
		Database = self.Database
		InsertSeq = "" if not InsertSeq else str(InsertSeq)
		InsertValue = str(InsertValue)
		Command = "INSERT INTO %s %s VALUES %s;" % (TableName, InsertSeq, InsertValue)
		self.Execute(Command)
		return Command

	def Query(self, TableName):
		# 未完成
		Database = self.Database
		Command = "SELECT * FROM %s" % TableName
		Result = self.Execute(Command)
		return Result

	def Commit(self):
		self.Database.commit()
		return True

	def GetConn(self):
		return self.Database

	def GetCursor(self):
		return self.Database.cursor()

	def allTables(self)
		Cursor = self.GetCursor()
		Cursor.execute(SELECT name FROM sqlite_master WHERE type='table';)
		TableTuple = Cursor.fetchall()
		self.TableList = [Element[0] for Element in TableTuple]
		return self.TableList

	def haveTable(self, TableName)
		TableList = self.Tables()
		if TableName in TableList
			return True
		return False

	def countRecordsFrom(self, TableName)
		Command = SELECT count() FROM %s % TableName
		Result = self.Execute(Command)
		Result = Result.fetchall()[0][0]
		return Result

	def countAllRecords(self):
		Result = 0
		for table in self.allTables():
			Result += self.countRecordsFrom(table)
		return Result

	def __del__(self):
		try:
			self.Database.close()
		except:
			pass