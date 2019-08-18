class Edge(object):
    def __init__(self,h=0,t=0,tn=0):
        self.__head = h
        self.__tail = t
        self.__targetNum = tn


    def get_head(self):
        return self.__head

    def get_taile(self):
        return self.__tail

    def get_targetNum(self):
        return self.__targetNum

    def set_head(self,value):
        self.__head = value

    def set_tail(self,value):
        self.__tail = value

    def set_targetNum(self,value):
        self.__targetNum = value
