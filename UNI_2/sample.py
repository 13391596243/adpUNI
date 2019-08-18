from dbClass import *
import shutil
import os
import random

db = Db()
#创建数据库表
db.createNodeTable()

#建立数据目录
filePath = '/hdb/students/cgr/UNI_500000'
nodeDir = '%s/targetNode' %filePath
folder = os.path.exists((nodeDir))
if not folder:
    os.makedirs(nodeDir)
recordFile = '%s/record.txt' %filePath

step = 1
targetStep = 0
TTR = 0.0
targetNode = []
while targetStep<=500000:
    id = random.randint(0,2*32-1)
    while db.getSampleID(id):
        id = random.randint(0,2**32-1)
    if db.testID(id):
        targetStep += 1
        targetNode.append(id)
        print("%d,%d target!" %(step,id))
    else:
        print("%d,%d no target!" %(step,id))
    if targetStep == 1000:
        with open('%s/%d.txt' %(nodeDir,targetStep),'w') as f:
            for ele in targetNode:
                f.write('%d\n' %ele)
    elif targetStep != 0 and targetStep % 1000 == 0:
        pstep = targetStep - 1000
        f1 = open('%s/%d.txt' % (nodeDir, pstep), 'r')
        f2 = open('%s/%d.txt' %(nodeDir,targetStep), 'w')
        shutil.copyfileobj(f1, f2)
        f1.close()
        f2.close()
        i = pstep
        with open(f2, 'a+') as f:
            while i < targetStep:
                f.write("%d\n" %targetNode[i])
                i += 1
    if step%1000 == 0:
        with open(recordFile,'a+') as f:
            TTR = round(targetStep/step,5)
            f.write('%d\t%d\t%f' %(step,targetStep,TTR))
with open(recordFile,'a+') as f:
    TTR = round(targetStep/step,5)
    f.write('%d\t%d\t%f' %(step,targetStep,TTR))
db.closeConn()

