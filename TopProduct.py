import operator

class TopProduct():
    def __init__(self, top_n = 5):
        self.data_dict = {}
        self.top_n = 5

    def add(self, pid, score):
        if ((pid not in self.data_dict) or (pid in self.data_dict and self.data_dict[pid] < score)):
            self.data_dict[pid] = score
            if (len(self.data_dict) > self.top_n):
                self.data_dict = dict(sorted(self.data_dict.items(), key=operator.itemgetter(1), reverse=True)[0:self.top_n])

    def get(self):
        return sorted(self.data_dict.items(), key=operator.itemgetter(1), reverse=True)