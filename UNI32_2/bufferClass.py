from basicClass import Interval

class Intervals(object):

    def __init__(self):
        self.__List = []

    def add_inter(self,interval):
        self.__List.append(interval)

    def get_interval(self,index):
        return self.__List[index]

    def write_intervals(self,filePath):
        i = 0
        with open(filePath,'w') as f:
            for interval in self.__List:
                f.write("%d\t%d\t%d\t%d\t%d\n" %(i,interval.get_lowBound(),interval.get_upBound(),interval.get_samNum(),interval.get_tarNum()))
                i += 1

class IDBuffer(object):
    def __init__(self):
        self.__targetID = []

    def get_targetLen(self):
        return len(self.__targetID)

    def add_tarID(self,id):
        self.__targetID.append(id)

    def write_ids(self,filePath,size):
        i = len(self.__targetID)-size
        with open(filePath,'a+') as f:
            while i<len(self.__targetID):
                f.write("%d\n" %self.__targetID[i])
                i+=1

# class EdgeBuffer(object):
#
#     def __init__(self):
#         self.__edges = []
#
#     def get_edgeSize(self):
#         return len(self.__edges)
#
#     def add_edge(self,edge):
#         self.__edges.append(edge)
#
#     def write_edges(self,filePath,size):
#         i = size
#         with open(filePath,'a+') as f:
#             while i<len(self.__edges):
#                 edge = self.__edges[i]
#                 f.write("%d\t%d\n" %(edge.get_head(),edge.get_tail()))
#                 i+=1