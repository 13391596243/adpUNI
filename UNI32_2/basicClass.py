class Interval(object):

    def __init__(self,index=0,lb=0,ub=0,tn=0,sn=0,sp=0.0):
        self.__index = index
        self.__lowBound = lb
        self.__upBound = ub
        self.__targetNum = tn
        self.__sampleNum = sn
        self.__samplePro = sp

    def get_tarNum(self):
        return self.__targetNum

    def set_tarNum(self):
        self.__targetNum += 1

    def get_samNum(self):
        return self.__sampleNum

    def set_samNum(self):
        self.__sampleNum += 1

    def get_lowBound(self):
        return self.__lowBound

    def set_lowBound(self,value):
        self.__lowBound = value

    def get_upBound(self):
        return self.__upBound

    def set_upBound(self, value):
        if value>(2**32-1):
            self.__upBound = 2**32-1
        else:
            self.__upBound = value

    def get_index(self):
        return self.__index

    def set_index(self, value):
        self.__index = value

    def get_samPro(self):
        return self.__samplePro

    def set_samPro(self,value):
        self.__samplePro = value

# class Edge(object):
#
#     def __init__(self,h=0,t=0,tn=0):
#         self.__head = h
#         self.__tail = t
#         self.__targetNum = tn
#
#     def get_head(self):
#         return self.__head
#     def set_head(self,value):
#         self.__head = value
#
#     def get_tail(self):
#         return self.__tail
#     def set_tail(self,value):
#         self.__tail = value
#
#     def get_tarNum(self):
#         return self.__targetNum
#     def set_tail(self,value):
#         self.__targetNum = value