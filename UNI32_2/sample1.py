from bufferClass import *
from dbClass import *
from basicClass import *
import shutil
import os
import random

class G:
    #输入
    tarStp = input("total target steps:")
    inteNum = input("the number of intervals divided:")
    recRate = input("the record rate:")
    #采样次数
    targetSteps = int(tarStp)
    #划分的区间数
    intervalNum = int(inteNum)
    #采样概率最低阈值
    alpha = round(1/intervalNum,5)
    recordRate = int(recRate)
    intervalLength = 2**32//intervalNum

    #缓存区声明
    tarIDLists = IDBuffer()
    intervalLists = Intervals()
    #edgeLists = EdgeBuffer()

    #数据目录声明
    dirName = '%s_%s' %(tarStp,inteNum)
    filePath = 'H:\\testData\\UNI32_2\\%s' %dirName
    nodeDir = '%s\\targetNode' %filePath
    intervalDir = '%s\\interval' %filePath
    recordTxt = '%s\\record.txt' %filePath

    db = Db()

    targetStep = 0
    step = 1

#创建数据库表
def create_db():
    #创建采样数据库表以实现不重复采样
    G.db.createNodeTable(G.targetSteps,G.intervalNum)
    #创建区间表
    G.db.createIntervalTable(G.targetSteps,G.intervalNum)
    print("Tables are created successfully!")

#创建文件夹
def mkdir():
    folder1 = os.path.exists(G.nodeDir)
    if not folder1:
        os.makedirs(G.nodeDir)

    folder2 = os.path.exists(G.intervalDir)
    if not folder2:
        os.makedirs(G.intervalDir)

    print("folders are made successfully!")

#初始化区间数组
def divideInterval():
    i = 0
    while i<=G.intervalNum:
        aInterval = Interval()
        aInterval.set_index(i)
        aInterval.set_lowBound(i*G.intervalLength)
        aInterval.set_upBound(aInterval.get_lowBound()+G.intervalLength)
        aInterval.set_samPro(G.alpha)
        G.intervalLists.add_inter(aInterval)
        G.db.insertInterval(G.targetSteps,G.intervalNum,aInterval)
        i += 1

def record1():
    nodePath = '%s\\%d.txt' % (G.nodeDir, G.targetStep)
    if G.targetStep == G.recordRate:
        G.tarIDLists.write_ids(nodePath, G.recordRate)
    elif G.targetStep != 0 and G.targetStep % G.recordRate == 0:
        pstep = G.targetStep - G.recordRate
        f1 = open('%s\\%d.txt' % (G.nodeDir, pstep), 'r')
        f2 = open(nodePath, 'w')
        shutil.copyfileobj(f1, f2)
        f1.close()
        f2.close()
        G.tarIDLists.write_ids(nodePath, G.recordRate)

def record2():
    if G.step % 100 == 0:
        with open(G.recordTxt, 'a+') as f:
            TTR = round(G.targetStep / G.step, 5)
            f.write('%d\t%d\t%f\n' % (G.step,G.targetStep,TTR))
    if G.step % 100 == 0:
        G.intervalLists.write_intervals('%s\\%d.txt' %(G.intervalDir,G.step))

#采样到一个节点时需要判断其是否命中及更新相应区间的属性
def id_do(id):
    interIndex = id//G.intervalLength
    aInterval = G.intervalLists.get_interval(interIndex)
    nodeFlag = False

    if G.db.testID(id):
        aInterval.set_tarNum()  #修改命中次数
        G.tarIDLists.add_tarID(id)
        G.targetStep += 1
        nodeFlag = True
        print("%d,%d target!" %(G.step,id))
        record1()
    else:
        print("%d,%d no target!" %(G.step,id))
    aInterval.set_samNum() #修改采样次数
    b = round(aInterval.get_tarNum()/aInterval.get_samNum()*(1-aInterval.get_samNum()/(aInterval.get_upBound()-aInterval.get_lowBound())),5)
    if b<G.alpha:
        b = G.alpha
    # 修改采样概率
    aInterval.set_samPro(b)
    #更新数据库内的区间数据
    G.db.updateInterval(G.targetSteps,G.intervalNum,interIndex,aInterval.get_samNum(),aInterval.get_tarNum(),b)
    G.step += 1
    record2()
    return nodeFlag

create_db()
mkdir()
divideInterval()
flag = False #为假则在小区间内采样，为真则在整个区间内采样
while G.targetStep<G.targetSteps:
    if flag:
        #整个空间内采样
        id = random.randint(0,2**32-1)
        #实现无放回采样
        while G.db.getSampleID(id,G.targetSteps,G.intervalNum):
            id = random.randint(0,2**32-1)
        G.db.insertSampleID(id,G.targetSteps,G.intervalNum)
        #如果节点命中，则获取邻居节点也加入样本
        if id_do(id):
            neighbours = G.db.getNeighbours(id)
            for ele in neighbours:
                if not G.db.getSampleID(ele, G.targetSteps, G.intervalNum):
                    G.db.insertSampleID(ele,G.targetSteps, G.intervalNum)
                    id_do(ele)
        flag = False
    else:
        #小区间内采样
        nodeFlag = False
        #获取按照采样概率排序的区间列表
        samProLists = G.db.get_samProLists(G.targetSteps,G.intervalNum)
        targetIndex = -1
        #寻找满足采样条件的区间
        for ele in samProLists:
            RP = random.random()
            if RP<=ele[1]:
                targetIndex = ele[0]
                break
        if targetIndex>-1 and targetIndex<len(samProLists):
            aInterval = G.intervalLists.get_interval(targetIndex)
            id = random.randint(aInterval.get_lowBound(),aInterval.get_upBound()-1)
            while G.db.getSampleID(id,G.targetSteps,G.intervalNum):
                id = random.randint(aInterval.get_lowBound(),aInterval.get_upBound()-1)
            G.db.insertSampleID(id, G.targetSteps, G.intervalNum)
            if G.db.testID(id):
                G.tarIDLists.add_tarID(id)
                G.targetStep += 1
                aInterval.set_tarNum()
                print('%d,%d target!' %(G.step,id))
                record1()
                nodeFlag = True
            else:
                print('%d,%d no target!' % (G.step, id))
            aInterval.set_samNum()
            b = round(aInterval.get_tarNum() / aInterval.get_samNum() * (
                        1 - aInterval.get_samNum() / (aInterval.get_upBound() - aInterval.get_lowBound())), 5)
            if b < G.alpha:
                b = G.alpha
            aInterval.set_samPro(b)
            G.db.updateInterval(G.targetSteps, G.intervalNum, targetIndex, aInterval.get_samNum(),
                                aInterval.get_tarNum(), b)
            G.step += 1
            record2()
            if nodeFlag:
                neighbours = G.db.getNeighbours(id)
                for ele in neighbours:
                    if not G.db.getSampleID(ele,G.targetSteps,G.intervalNum):
                        G.db.insertSampleID(ele, G.targetSteps, G.intervalNum)
                        id_do(ele)
        else:
            flag = True
G.db.closeConn()
with open(G.recordTxt, 'a+') as f:
    TTR = round(G.targetStep / (G.step-1), 5)
    f.write('%d\t%d\t%f\n' % (G.targetStep,  (G.step-1), TTR))
G.intervalLists.write_intervals('%s%d.txt' % (G.intervalDir,  (G.step-1)))



            




