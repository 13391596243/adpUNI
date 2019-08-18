from dbClass import *
import shutil
import os
import random

class G:
    db = Db()
    targetStep = 0
    # 建立数据目录
    filePath = '/home/students/cgr/UNI_2_data/test'
    nodeDir = '%s/targetNode.txt' %filePath
    # recordFile = '%s/record.txt' %filePath

    if not os.path.exists(filePath):
        os.makedirs(filePath)

    nf = open(nodeDir,'w')

while G.targetStep<=500000:
    Id = random.randint(0,2**32-1)
    if G.db.testID(Id):
        G.targetStep += 1
        print("%d,%d target!" % (G.targetStep, Id))
        G.nf.write("%d\n" %Id)
        G.nf.flush()
G.nf.close()
G.db.closeConn()