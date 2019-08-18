from pymysql import *

class G:
    seed = input("please input the first node:")
    seed = int(seed)

    dirName = "BFS_%s" %seed
    filePath = "/home/students/Alpha/BFS_data/%s" %dirName
    nodefile = "%s/targetNode.txt" %filePath
    # edgefile = "%s/edges.txt" %filePath

    nf = open(nodefile,'w')
    # ef = open(edgefile,'w')

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

nodeQueue = []
step = 0
nodeQueue.append(G.seed)
nodeSet = set()
while step<=500000 or nodeQueue:
    id = nodeQueue.pop(0)
    if id not in nodeSet:
        G.nf.write('%d\n' %id)
        G.nf.flush()
        nodeSet.add(id)
        step += 1
        neighbors = getNeighbours(id)
        while neighbors:
            for ele in neighbors:
                nodeQueue.append(ele)
G.nf.close()