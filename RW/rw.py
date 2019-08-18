from pymysql import *
import os
import random

class G:
    seed = input("please input the first node:")
    seed = int(seed)

    dirName = "RW_%s" %seed
    filePath = "/home/students/Alpha/RW_data/%s" %dirName
    nodefile = "%s/targetNode.txt" %filePath
    edgefile = "%s/edges.txt" %filePath

    if not os.path.exists(filePath):
        os.makedirs(filePath)

    nf = open(nodefile,'w')
    ef = open(edgefile,'w')

    conn = connect(host='localhost', port=3306, db='crawled_weibo_smaller', user='students',
                   passwd='Students@228', charset='utf8')

def getNeighbours(id):
    myCursor = G.conn.cursor()
    sql1 = "select tuid from edges where suid=%d" %id
    sql2 = "select suid from edges where tuid=%d" %id
    neiList = []
    try:
        myCursor.execute(sql1)
        result1 = myCursor.fetchall()
        for data in result1:
            neiList.append(data[0])
        myCursor.execute(sql2)
        result2 = myCursor.fetchall()
        for data in result2:
            if data[0] not in neiList:
                neiList.append(data[0])
        myCursor.close()
        return neiList
    except Exception as e:
        print(e)

step = 1
Id = G.seed
print(step,Id)
G.nf.write('%d\n' %Id)
G.nf.flush()
neighbours = getNeighbours(Id)
while step <= 500000:
    if neighbours:
        index = random.randint(0,len(neighbours)-1)
        nextId = neighbours[index]
        G.ef.write('%d,%d\n' %(Id,nextId))
        G.ef.flush()
        Id = nextId
        step += 1
        print(step, Id)
        G.nf.write('%d\n' % Id)
        G.nf.flush()
        neighbours = getNeighbours(Id)
    else:
        print("Neighbour list is empty!")
G.ef.close()
G.nf.close()















