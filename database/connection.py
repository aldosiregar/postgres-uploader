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
    
    addNewDatabase(dbname)
        membuat database baru pada server postgreSQL

    changeDatabase(host, dbname, user, password)
        pindah dari database yang sekarang kita kerjakan

    addNewTableToDatabase(tablename, column)
        membuat table baru pada database kita

    addNewDataToTable(tablename, data)
        menambahkan data baru ke table tertentu 

    retrieveAllData(self, tablename)
        mengambil semua data pada database tertentu

    overwriteDataFromDatabase(tablename, data)
        menghapus semua data dan memperbarui data di database
    
    terminate()
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
        """
        try:
            #membuat koneksi ke database
            self.conn = psycopg2.connect(
                """host=%s dbname=%s user=%s password=%s""" % (host, dbname, user, password)
                )
            self.cur = self.conn.cursor()
            #perubahan langsung disimpan ke database tanpa harus commit
            self.conn.set_session(autocommit=True)
        except psycopg2.Error as error:
            print("cant connect to postgresql")
            raise error
        
        print("berhasil terhubung ke database")

    def addNewDatabase(self, dbname=str):
        """
        membuat database baru pada postgreSQL

        Parameters :
        ---------

        dbname : str
            nama dari database baru yang akan kita buat
        """
        #membuat database baru
        try:
            self.cur.execute("""CREATE DATABASE %s""" % dbname)
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
        """
        #hapus koneksi
        try:
            self.conn.close()
        except psycopg2.Error as error:
            print(error)

        try:
            #membuat koneksi ulang ke database
            self.conn = psycopg2.connect(
                """host=%s dbname=%s user=%s password=%s""" 
                % (host, dbname, user, password)
                )
        except psycopg2.Error as error:
            print("cant connect to postgresql")
            print(error)
        
        #inisiasi cursor baru untuk dikembalikan ke file model
        try:
            self.cur = self.conn.cursor()
        except psycopg2.Error as error:
            print(error)
        
        #tentukan ketentuan session menjadi autocommit
        self.conn.set_session(autocommit=True)
    
    def addNewTableToDatabase(self, tablename=str, column=dict):
        """
        membuat table baru pada database yang sedang dikerjakan

        Parameters :
        ---------
        
        tablename : str
            nama dari tabel yang ingin dibuat
        
        column : dict
            kolom dan tipe data yang diharapkan pada tabel baru
        
        :return: str
            status penambahan data berhasil atau gagal
        """
        keysInDict = list(column.keys())
        valuesInDict = list(column.values())

        #jika table bisa dibuat, query ini tidak akan dijalankan
        try:
            self.cur.execute(
                """CREATE TABLE IF NOT EXISTS %s""" % (tablename) + """ (""" + 
                """,""".join(
                    ["""%s %s""" % 
                    tuple([key, value]) for (key, value) in zip(keysInDict, valuesInDict)])
                + """);"""
                )
        except psycopg2.Error as error:
            print("terjadi error")
            print(error)
            return None
        
        return "tabel berhasil dibuat"

    #menambahkan data baru ke table
    def addDataToTable(self, tablename=str, data=dict):
        """
        menambahkan data baru ke table

        Parameter :
        ---------

        tablename : str
            nama dari table yang ingin ditambah datanya

        data : dict
            data yang ingin ditambahkan
            example : 
            {"column1" : {0 : 1, 1 : 2, 2 : 3}, "column2" : {0 : 4, 1 : 5, 2 : 6}}
            or
            {"column1" : [0 : 1, 1 : 2, 2 : 3], "column2" : [0 : 4, 1 : 5, 2 : 6]}
        """

        #berapa banyak keys pada data
        keysInData = list(data.keys())

        keySize = None

        #membuat format string untuk mencegah terjadinya sql injection
        #menggabungkan list comprehension dan str.join untuk membuat query
        queryString = (
            """INSERT INTO %s (""" % tablename + 
            (
                """,""".join(["""%s""" for _ in range(len(keysInData))]) % 
                tuple(keysInData)
            ) + """) VALUES """
        )

        try:
            #index jika data didalam kolom semua berbentuk dict
            keySize = list(data[keysInData[0]].keys())
        except AttributeError:
            #iterator pada jumlah keys dari data 
            keySize = range(len(keysInData))
        except:
            print("you submited wrong format")
            return None

        #cur.mogrify() untuk memasukkan banyak value dalam satu query
        temp = ( """(""" +
            """,""".join(["%s" for _ in range(len(keysInData))])
        ) + """)"""
        dataInList = [
            tuple([data[columns][rows] for columns in keysInData])
            for rows in keySize
            ]


        #parsing semua data yang akan dimasukkan ke table
        argqs = """,""".join(self.cur.mogrify(temp, x).decode("utf-8") for x in dataInList)

        #eksekusi query penambah data ke cursor
        try:
            self.cur.execute(queryString + argqs + """;""")
            print("data's is succesfully added")
        except psycopg2.Error as error:
            print("data can't be added")
            print(error)

    def retrieveAllDataFromDatabase(self, tablename=str):
        """
        mengambil semua data pada database tertentu

        Parameter : 
        ---------

        tablename : str
            nama dari tabel yang ingin diambil datanya
            
        :return : tuple
        """
        result = None
        try :
            self.cur.execute(
                """SELECT * FROM %s;""" % tablename
            )
            result = self.cur.fetchall()
        except psycopg2.Error as error:
            print("error happen when retrieve data")
            raise error
        
        return result
        
    def overwriteDataFromDatabase(self, tablename=str, data=dict):
        """
        menghapus semua data dari database, lalu memasukkannya kembali

        Parameter :
        ---------

        tablename : str
            nama dari tabel yang ingin diubah datanya

        data : dict
            data yang akan dimasukkan ke tabel

        datasize : int
            jumlah dari data yang akan dimasukkan ke database
        """
        query = """DELETE FROM %s""" % tablename
        try:
            self.cur.execute(query=query)
            self.addDataToTable(tablename=tablename, data=data)
        except psycopg2.Error as error:
            print("terjadi error saat memperbarui data")
            print(error)

    #ketika object dihapus, hapus koneksi menuju database
    def terminate(self):
        """
        untuk memutus koneksi pada database
        """
        #hapus koneksi
        try:
            self.cur.close()
            self.conn.close()
        except psycopg2.Error as error:
            print(error)