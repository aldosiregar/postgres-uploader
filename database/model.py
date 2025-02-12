import connection

#kelas kedua yang mengurusi tentang 
class connectionToDataBase():
    """
    kelas yang bertugas untuk memanipulasi data saja dari dan ke database

    Attribute : 
    ---------

    host : str
        host yang saat ini digunakan untuk konektivitas database

    user : str
        user yang saat ini digunakan untuk konektivitas database

    databaseConnection : connection.dbConnection()
        object dari kelas dbConnection

    cur : psycopg2.connect().cursor()
        cursor yang akan digunakan untuk menjalankan query string

    method :
    ------

    newDatabase(dbname)
        membuat database baru 

    changeDatabaseUsed(dbname, password)
        merubah database yang akan dikerjakan

    addNewData(tablename, data)
        menambahkan data baru ke table tertentu (masih prototype, ada perubahan nanti)

    addNewDataToTable(tablename, data)
        menambahkan data baru ke table tertentu (fungsi penambah data sebenarnya)

    __del__()
        hilangkan semua koneksi ke database serta menghapus semua informasi dari ram
    """
    #inisiasi
    databaseConnection = connection.dbConnection()

    def __init__(self, host=str, dbname=str, user=str, password=str):
        """
        constructor untuk connectionToDatabase

        Parameters : 
        ----------

        host : str 
            port host dari database yang akan dikoneksi 

        dbname : str  
            database dari database yang akan dikoneksi 

        user : str  
            user dari database yang akan dikoneksi 

        password : str 
            password dari postgreSQL agar bisa mengakses database
        """
        #menginisiasi koneksi dari parent
        self.host = host
        self.user = user
        self.cur = self.databaseConnection.makeConnection(
            host=host ,dbname=dbname, user=user, password=password
            )

    #membuat table baru
    def newDatabase(self, dbname=str):
        """
        membuat database baru

        Parameter :
        ---------

        dbname : str
            nama dari database baru
        """
        self.databaseConnection.addNewDatabase(cur=self.cur, dbname=dbname)

    #mengganti table yang sekarang dikerjakan
    def changeDatabaseUsed(self, dbname=str, password=str):
        """
        mengganti database yang akan dikerjakan

        Parameter :
        ---------

        dbname : str
            nama dari database yang akan digunakan
        
        password : str
            password dari postgreSQL
        """
        self.cur = self.databaseConnection.changeDatabase(
            host=self.host, tablename=dbname, user=self.user, password=password
            )

    #membuat table baru pada database
    def addNewData(self, tablename=str, data=dict, datasize=int):
        """
        (prototype) menambah data baru ke table, mungkin ada perubahan kedepannya

        Parameter :
        ---------

        tablename : str
            nama dari table yang akan ditambahkan datanya

        data : dict
            data yang akan ditambahkan ke table 

        datasize : int
            jumlah dari berapa banyak baris data yang ingin ditambahkan
        """
        self.addDataToTable(tablename=tablename, data=data, dataSize=datasize)

    #menambahkan data baru ke table
    def addDataToTable(self, tablename=str, data=dict, dataSize=int):
        """
        menambahkan data baru ke table

        Parameter :
        ---------

        tablename : str
            nama dari table yang ingin ditambah datanya

        data : dict
            data yang ingin ditambahkan

        datasize : int
            jumlah dari data yang ingin ditambahkan
        """

        #berapa banyak keys pada data
        keysInData = list(data.keys())

        #iterator pada jumlah keys dari data
        keySize = range(len(keysInData))

        #querystring data
        queryString = "INSERT INTO " + tablename + " ("

        #berapa banyak kolom dalam table tersebut, untuk keperluan formating
        columnVariable = ""
        
        #tamabahkan kolom yang ingin ditambahkan ke dalam querystring 
        #dan berapa banyak formatornya
        for i in keysInData:
            queryString += i + ", "
            columnVariable += "%s,"

        #querystring disatukan dengan formator data
        queryString = queryString[:-2] + ") VALUES (" + columnVariable[:-1] + ")"

        #parsing semua data yang akan dimasukkan ke table
        for rows in range(dataSize):
            dataForTable = tuple([data[keysInData[columns]][rows] for columns in keySize])

            #eksekusi query penambah data ke cursor
            try:
                self.cur.execute(queryString, dataForTable)
            except:
                print("data cant be added")

    #destructor
    def __del__(self):
        self.databaseConnection.terminate(self.cur)
        del self.databaseConnection
        self.host = None
        self.user = None
        self.cur = None
        self.databaseConnection = None