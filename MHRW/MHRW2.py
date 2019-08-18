from pymysql import *
import os
import random
import shutil

class G:
    seed = input("please input the first node:")
    seed = int(seed)

    dirName = "MHRW_%s" %seed
    filePath = "/home/students/Alpha/MHRW_data/%s" %dirName
    repeat_nodeDir = "%s/repeatNode" %filePath
    unique_nodeDir = "%s/uniqueNode" %filePath
    recordTxt = "%s/record.txt" %filePath
    edgeDir = "%s/edges" %filePath

 

    conn = connect(host='localhost', port=3306, db='crawled_weibo_smaller', user='students',
                   passwd='Students@228', charset='utf8')

def getEdges():
    myCursor = G.conn.cursor()
    sql = "select * from edges"
    edgeDict = {}
    try:
        myCursor.execute(sql)
        result = myCursor.fetchall()
        for row in result:
            for u,v in row:
                if u not in edgeDict.keys():
                    edgeDict[u] = []
                else:
                    if v not in edgeDict[u]:
                        edgeDict[u].append(v)
                if v not in edgeDict.keys():
                    edgeDict[v] = []
                else:
                    if u not in edgeDict[v]:
                        edgeDict[v].append(u)
        myCursor.close()
        return edgeDict
    except Exception as e:
        print(e)

def mkdir():
    folder1 = os.path.exists(G.repeat_nodeDir)
    if not folder1:
        os.makedirs(G.repeat_nodeDir)

    folder2 = os.path.exists(G.unique_nodeDir)
    if not folder2:
        os.makedirs(G.unique_nodeDir)

    folder2 = os.path.exists(G.edgeDir)
    if not folder2:
        os.makedirs(G.edgeDir)

    print("folders are made successfully!")

# mkdir()
edges = getEdges()
G.conn.close()
repeatNode = []
uniqueNode = set()
targetEdge = []
step = 1
Id = G.seed
print(step,Id)
repeatNode.append(Id)
uniqueNode.add(Id)
neighbours = edges[Id]
while True:
    if neighbours:
        index = random.randint(0,len(neighbours)-1)
        testId = neighbours[index]
        testNeighbours = edges[testId]
        if testNeighbours:
            if len(neighbours)/len(testNeighbours) >= random.random():
                targetEdge.append([Id,testId])
                Id = testId
                neighbours = testNeighbours
                uniqueNode.add(Id)
                if len(uniqueNode)!=0 and len(uniqueNode)%1000==0:
                    with open('%s/%d.txt' %(G.unique_nodeDir,len(uniqueNode)),'w') as f:
                        for ele in uniqueNode:
                            f.write('%d\n' %ele)
                    with open('%s/%d.txt' %(G.edgeDir,len(uniqueNode)),'w') as f:
                        for ele in uniqueNode:
                            f.write('%d\t%d\n' %(ele[0],ele[1]))
                    
                if len(uniqueNode)!=0 and len(uniqueNode)%100000==0:
                    if input('stop or continue:')=='S':
                        break
            repeatNode.append(Id)
            step += 1
            print(step,Id)
            if step!=0 and step%1000 == 0:
                with open(recordTxt,'a+') as f:
                    f.write('%d\t%d\t%.f\n' %(step,len(uniqueNode),step/len(uniqueNode)))
            if step!=0 and step%10000 == 0:
                with open('%s/%d.txt' %(G.repeat_nodeDir,step,'w') as f:
                    for ele in repeatNode:
                        f.write('%d\n' %ele)
                repeatNode.clear()

    else:
        print("Neighbour list is empty!")
        exit()
















