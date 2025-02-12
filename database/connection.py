#import psycog2
import psycopg2

class dbConnection:
    #initiate sekaligus membuat koneksi ke database
    #return psycopg2.connection.cursor
    def makeConnection(self, host=str, dbname=str, user=str, password=str):
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

    #tambahkan database baru  
    def addNewDatabase(self, cur=psycopg2.connect().cursor, dbname=str):
        #membuat database baru
        try:
            cur.execute("CREATE DATABASE " + dbname)
        except psycopg2.Error as error:
            print(error)

    #pindah ke table lain
    #return psycopg2.conection
    def changeDatabase(self, host=str, dbname=str, user=str, password=str):
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
    
    def addNewTableToDatabase(self, cur=psycopg2.connect().cursor, tablename=str, column=dict):
        #jika table bisa dibuat
        try:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS " + tablename + "(" + 
                self.parseDictToString(column) + ");"
                ) 
        except psycopg2.Error as error:
            print("problem when creating table")
            print(error)

    #melakukan parsing agar semua data pada dictionary dapat menjadi string
    def parseDictToString(target=dict):
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
    def terminate(self, cur=psycopg2.connect().cursor):
        #hapus koneksi
        try:
            cur.close()
            self.conn.close()
        except psycopg2.Error as error:
            print(error)