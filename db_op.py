import MySQLdb


class DB:
    conn = None

    def connect(self):
        self.conn = MySQLdb.connect("127.0.0.1", "root", "13610522")

    def query(self, sql):
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
        except (AttributeError, MySQLdb.OperationalError):
            self.connect()
            cursor = self.conn.cursor()
            cursor.execute(sql)
        return cursor

# db = DB()
# sql = "select * from mmp.services"
# cur = db.query(sql)
# # wait a long time for the Mysql connection to timeout
# # cur = db.query(sql)
# print cur.fetchone()
