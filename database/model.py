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

    method :
    ------

    newDatabase(dbname)
        membuat database baru 

    changeDatabaseUsed(dbname, password)
        merubah database yang akan dikerjakan
    
    def addNewTable(self, tablename, column)
        menambahkan tabel baru pada database

    addNewData(tablename, data)
        menambahkan data baru ke table tertentu (masih prototype, ada perubahan nanti)

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
        self.databaseConnection.makeConnection(
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
        self.databaseConnection.changeDatabase(
            host=self.host, tablename=dbname, user=self.user, password=password
        )
        
    def addNewTable(self, tablename=str, column=dict):
        """
        menambahkan tabel baru ke dalam database

        Parameter : 
        ---------

        tablename : str
            nama dari tabel yang akan dibuat 

        column : dict
            kolom yang akan dibuat akan menampung \n
            data apa, keys dan values nya berbentuk string
            example :
            {"column1" : "int", "column2" : "varchar(50)", "column3" : "date"}
        """
        self.databaseConnection.addNewTableToDatabase(
            self.cur, tablename=tablename, column=column
        )

    #membuat table baru pada database
    def addNewData(self, tablename=str, data=dict, datasize=int):
        """
        menambah data baru ke table, mungkin ada perubahan kedepannya

        Parameter :
        ---------

        tablename : str
            nama dari table yang akan ditambahkan datanya

        data : dict
            data yang akan ditambahkan ke table 

        datasize : int
            jumlah dari berapa banyak baris data yang ingin ditambahkan
        """
        result = self.databaseConnection.addDataToTable(
            tablename=tablename, data=data, dataSize=datasize
            )
        
        print(result)

    #ubah data yang sudah diambil dari database menjadi dict
    def retrieveAllData(self):
        return None
    
    #destructor
    def __del__(self):
        self.databaseConnection.terminate(self.cur)
        del self.databaseConnection
        self.host = None
        self.user = None
        self.cur = None
        self.databaseConnection = None