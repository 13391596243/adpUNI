from pymysql import *
from basicClass import *

class Db(object):
    def __init__(self):
        self.conn = connect(host='222.199.225.228',port=3306,db='crawled_weibo_smaller',user='students',passwd='Students@228',charset='utf8')

    def createNodeTable(self,targetStep,intervalNum):
        self.cursor = self.conn.cursor()
        tableName = 'sam_%d_%d'%(targetStep,intervalNum)
        sql1 = "DROP TABLE IF EXISTS %s" %tableName
        sql2 = "CREATE TABLE %s (uid INT UNSIGNED PRIMARY KEY)"%tableName
        try:
            self.cursor.execute(sql1)
            self.cursor.execute(sql2)
            self.conn.commit()
            self.cursor.close()
        except Exception as e:
            print(e)

    def createIntervalTable(self,targetStep,intervalNum):
        self.cursor = self.conn.cursor()
        tableName = 'inter_%d_%d'%(targetStep,intervalNum)
        sql1 = "DROP TABLE IF EXISTS %s"%tableName
        sql2 = "CREATE TABLE %s (inteIndex INT PRIMARY KEY,low INT UNSIGNED,up INT UNSIGNED,targetNum INT,sampleNum INT,samplePro DOUBLE(6,5),INDEX (samplePro))"%tableName
        try:
            self.cursor.execute(sql1)
            self.cursor.execute(sql2)
            self.conn.commit()
            self.cursor.close()
        except Exception as e:
            print(e)

    def testID(self,id):
        self.cursor = self.conn.cursor()
        sql = "SELECT * FROM nodes WHERE uid='%d'" %id
        flag = False
        try:
            self.cursor.execute(sql)
            self.cursor.fetchall()
            if self.cursor.rowcount==1:
                flag = True
            self.cursor.close()
        except Exception as e:
            print(e)
        return flag

    def insertSampleID(self,id,targetStep,intervalNum):
        self.cursor = self.conn.cursor()
        flag = True
        tableName = 'sam_%d_%d' % (targetStep, intervalNum)
        sql = "INSERT INTO %s (uid) VALUES (%d)" % (tableName, id)
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            self.cursor.close()
        except Exception as e:
            print(e)
        return flag

    def insertInterval(self,targetStep,intervalNum,aInterval):
        self.cursor = self.conn.cursor()
        tableName = 'inter_%d_%d' %(targetStep,intervalNum)
        sql = "INSERT INTO %s (inteIndex,low,up,targetNum,sampleNum,samplePro) VALUES ('%d','%d','%d','%d','%d','%f')" %(tableName,aInterval.get_index(),aInterval.get_lowBound(),aInterval.get_upBound(),aInterval.get_tarNum(),aInterval.get_samNum(),aInterval.get_samPro())
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            self.cursor.close()
        except Exception as e:
            print(e)

    def updateInterval(self,targetStep,intervalNum,interIndex,samNum,tarNum,samPro):
        self.cursor = self.conn.cursor()
        tableName = 'inter_%d_%d' % (targetStep, intervalNum)
        sql = "UPDATE %s SET sampleNum=%d,targetNum=%d,samplePro=%f WHERE inteIndex=%d" %(tableName,samNum,tarNum,samPro,interIndex)
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            self.cursor.close()
        except Exception as e:
            print(e)

    def getNeighbours(self,id):
        self.cursor = self.conn.cursor()
        sql1 = "SELECT suid FROM edges WHERE tuid=%d" %id
        sql2 = "SELECT tuid FROM edges WHERE suid=%d" %id
        neighbour = set()
        try:
            self.cursor.execute(sql1)
            result1 = self.cursor.fetchall()
            for row in result1:
                neighbour.add(row[0])
            self.cursor.execute(sql2)
            result2 = self.cursor.fetchall()
            for row in result2:
                neighbour.add(row[0])
            return neighbour
        except Exception as e:
            print(e)

    def getSampleID(self,id,targetStep,intervalNum):
        self.cursor = self.conn.cursor()
        tableName = 'sam_%d_%d' % (targetStep, intervalNum)
        sql = "SELECT * FROM %s WHERE uid=%d" % (tableName, id)
        flag = False
        try:
            self.cursor.execute(sql)
            self.cursor.fetchall()
            if self.cursor.rowcount>0:
                flag = True
            self.cursor.close()
        except Exception as e:
            print(e)
        return flag

    def get_samProLists(self,targetStep,intervalNum):
        self.cursor = self.conn.cursor()
        tableName = 'inter_%d_%d' % (targetStep, intervalNum)
        sql = "SELECT inteIndex,samplePro FROM %s ORDER BY samplePro DESC" %tableName
        samProLists = []
        try:
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            for row in results:
                samProLists.append([row[0],row[1]])
            self.cursor.close()
            return samProLists
        except Exception as e:
            print(e)

    def getIDs(self):
        self.cursor = self.conn.cursor()
        sql = "SELECT uid FROM nodes"
        IDLists = set()
        try:
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            for row in results:
                IDLists.add(row[0])
            self.cursor.close()
            return IDLists
        except Exception as e:
            print(e)

    def closeConn(self):
        self.conn.close()

