import DataBaseManager
DBHandler = DataBaseManager.DBHandler("UNIDATABASE")
TableList = DBHandler.Tables()
Count = 0
for Table in TableList:
	Count += DBHandler.Count(Table)
print (Count)