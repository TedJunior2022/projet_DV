import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dbServer import DbServer
import os

class PostgreDbServer(DbServer):

    def connectTOdbComplete(self, db_name, user, pwd):
        # Establish a Postgre connection
        database = psycopg2.connect("dbname="+db_name+" user="+user+ " password="+pwd)
        return database

    def connectTOdb(self, user, pwd):
        # Establish a Postgre connection
        database = psycopg2.connect("user="+user+" password=" +pwd)
        return database

    def executeRequest(self, database, request):
        database.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);
        cursor = database.cursor()
        status = cursor.execute(request)
        # Close the cursor
        cursor.close()
        # Commit the transaction
        database.commit()
        return status

    def checkIfDBexists(self,database, db_name):
        cursor = database.cursor()
        cursor.execute('select coalesce((SELECT \'OK\' FROM pg_database where datname = \'' + db_name + '\'), \'KO\')')
        out = cursor.fetchone()
        return out

    def checkIfTableExists(self,database, table):
        cursor = database.cursor()
        cursor.execute('select coalesce((SELECT \'OK\' FROM pg_tables where tablename = \'' + table+ '\'), \'KO\')')
        out = cursor.fetchone()
        return out[0]

    def loadFile(self, database, file_name, schema_name, table_name, delimiter):
        cursor = database.cursor()
        cursor.execute('COPY ' + schema_name+'.'+table_name+' FROM \'' + file_name+'\' DELIMITER \'' +delimiter+'\' CSV HEADER')
    
    def DropStagingTable(self, database, table):
        cursor = database.cursor()
        cursor.execute('Drop Table If Exists ' + table)