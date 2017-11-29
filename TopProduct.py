import operator

class TopProduct():
    def __init__(self):
        self.top = {} # pid : (score, status_default)

    def add(self, pid, score, default = False):
        if ((pid not in self.top) or
            (pid in self.top and self.top[pid][0] < score) or
            (pid in self.top and self.top[pid][1] == True)):
            self.top[pid] = (score, default)

    def get(self):
        return sorted(self.top.items(), key=operator.itemgetter(1), reverse=True)

    def getTop(self, top_n = 5):
        return self.get()[0:top_n]
