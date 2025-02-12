#import psycopg2
import psycopg2

class dbConnection:
    """
    class untuk membuat koneksi ke database, dan hanya mengurusi \n 
    hal sensitif seperti pembuatan database, table baru

    Attribute :
    ---------

    conn : psycopg2.connect()
        menyimpan koneksi conn yang terhubung pada database postgreSQL 

    Method : 
    ------

    makeConnection(host, dbname, user, password) 
        methods yang nantinya akan membuat koneksi ke database 
    
    addNewDatabase(cur, dbname)
        membuat database baru pada server postgreSQL

    changeDatabase(host, dbname, user, password)
        pindah dari database yang sekarang kita kerjakan

    addNewTableToDatabase(cur, tablename, column)
        membuat table baru pada database kita
    
    parseDictToString(target)
        merubah dict menjadi format string
    
    terminate(cur)
        mematikan koneksi ke database postgreSQL
    """

    def makeConnection(self, host=str, dbname=str, user=str, password=str):
        """
        membuat koneksi ke database

        Parameters : 
        ----------

        host : str 
            port host dari database yang akan dikoneksi 

        dbname : str  
            database dari database yang akan dikoneksi 

        user : str  
            user dari database yang akan dikoneksi 

        password : str 
            password dari postgreSQL agar bisa menggunakan query

        Return : 
        ------

        cur : psycopg2.connect.cursor() 
            cursor hasil koneksi yang akan digunakan untuk memproses query
        """
        try:
            #membuat koneksi ke database
            self.conn = psycopg2.connect(
                "host=" + host + " dbname=" + dbname + " user=" + user + " password=" + password
                )
            cur = self.conn.cursor()
            #perubahan langsung disimpan ke database tanpa harus commit
            self.conn.set_session(autocommit=True)
        except psycopg2.Error as error:
            print("cant connect to postgresql")
            print(error)
        
        return cur

    def addNewDatabase(self, cur=psycopg2.connect().cursor(), dbname=str):
        """
        membuat database baru pada postgreSQL

        Parameters :
        ---------

        cur : psycopg2.connect().cursor()
            cursor yang akan mengeksekusi query pada postgreSQL

        dbname : str
            nama dari database baru yang akan kita buat
        """
        #membuat database baru
        try:
            cur.execute("CREATE DATABASE " + dbname)
        except psycopg2.Error as error:
            print(error)

    def changeDatabase(self, host=str, dbname=str, user=str, password=str):
        """
        pindah ke table lain yang ada pada database yang sedang dikerjakan

        Parameters : 
        ---------

        host : str
            port host dari database yang akan digunakan

        dbname : str
            nama dari database yang akan digunakan

        user : str
            user dari database yang akan digunakan

        password : str
            password dari postgreSQL agar bisa mengakses database

        Return :
        ------

        cur : psycopg2.connect().cursor()
            cursor untuk melakukan proses query
        """
        #hapus koneksi
        try:
            self.conn.close()
        except psycopg2.Error as error:
            print(error)

        try:
            #membuat koneksi ulang ke database
            self.conn = psycopg2.connect(
                "host=" + host + " dbname=" + dbname + " user=" + user + " password=" + password
                )
        except psycopg2.Error as error:
            print("cant connect to postgresql")
            print(error)
        
        #inisiasi cursor baru untuk dikembalikan ke file model
        try:
            cur = self.conn.cursor()
        except psycopg2.Error as error:
            print(error)
        
        #tentukan ketentuan session menjadi autocommit
        self.conn.set_session(autocommit=True)
        
        return cur
    
    def addNewTableToDatabase(self, cur=psycopg2.connect().cursor(), tablename=str, column=dict):
        """
        membuat table baru pada database yang sedang dikerjakan

        Parameters :
        ---------

        cur : psycopg2.connect().cursor()
            cursor yang digunakan untuk menjalankan query
        
        tablename : str
            nama dari tabel yang ingin dibuat
        
        column : dict
            kolom dan tipe data yang diharapkan pada tabel baru
        """
        #jika table bisa dibuat, query ini tidak akan dijalankan
        try:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS " + tablename + "(" + 
                self.parseDictToString(column) + ");"
                ) 
        except psycopg2.Error as error:
            print("table sudah ada pada database")
            print(error)

    #melakukan parsing agar semua data pada dictionary dapat menjadi string
    def parseDictToString(target=dict):
        """
        melakukan parsing pada dictionary menjadi string

        Parameters :
        ---------

        target : dict
            dictionary yang ingin dijadikan string
        
        Return :
        ------
        
        parsed : str
            string hasil parsing dari dictionary menjadi query string
        """
        keysInDict = list(target.keys())
        parsed = str([
            str(keysInDict[i]) + " " + str(target[keysInDict[i]]) + ", " for i in range(len(keysInDict))
            ])
        
        temp = ""
        for i in parsed:
            temp += i
        parsed = temp[:-2]

        return parsed
    
    #ketika object dihapus, hapus koneksi menuju database
    def terminate(self, cur=psycopg2.connect().cursor()):
        """
        untuk memutus koneksi pada database

        Parameters :
        ---------

        cur : psycopg2.connect().cursor()
            cursor untuk menjalankan query
        """
        #hapus koneksi
        try:
            cur.close()
            self.conn.close()
        except psycopg2.Error as error:
            print(error)