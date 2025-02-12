import connection

#kelas kedua yang mengurusi tentang 
class connectionToDataBase(connection.dbConnection):
    #inisiasi
    def __init__(self, host=str, dbname=str, user=str, password=str):
        #menginisiasi koneksi dari parent
        self.host = host
        self.user = user
        self.cur = self.makeConnection(host=host ,dbname=dbname, user=user, password=password)

    #membuat table baru
    def addNewDatabase(self, dbname=str):
        self.addNewDatabase(cur=self.cur, dbname=dbname)

    #mengganti table yang sekarang dikerjakan
    def changeDatabaseUsed(self, dbname=str, password=str):
        self.cur = self.changeDatabase(host=self.host, tablename=dbname, user=self.user, password=password)

    #membuat table baru pada database
    def addNewTable(self, data=dict):
        self.addDataToTable(data)


    #menambahkan data baru ke table
    def addDataToTable(self, data=dict, dataSize=int):
        keysInData = list(data.keys())
        keySize = range(len(keysInData))
        columnParsed = "("
        
        #parsing semua kolom yang akan dimasukkan data
        for i in keysInData:
            columnParsed += i + ", "

        columnParsed = columnParsed[:-2] + ") \\ VALUES "

        #parsing semua data yang akan dimasukkan ke table
        for rows in range(dataSize):
            temp = columnParsed + "("
            for columns in keySize:
                temp += data[keysInData[columns]][rows] + ", "
            temp = temp[:-2] + ");"

            #eksekusi query penambah data ke cursor
            try:
                self.cur.execute(temp)
            except:
                print("data cant be added")


    #terminate connection
    def terminateConnection(self):
        self.terminate(self.cur)