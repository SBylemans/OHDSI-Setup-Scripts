import psycopg2 as psql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class DataBase:
    def createConnection(self, dbName, username, password, host, port):
        self.con = psql.connect(dbname=dbName, user=username, host=host, password=password, port=port)
        self.con.autocommit = True

    def executeCommand(self, command):
        cur = self.con.cursor()
        cur.execute(command)
        self.con.commit()

        print(command)
        print("\n---> Executed\n\n")

    def close(self):
        self.con.close()
