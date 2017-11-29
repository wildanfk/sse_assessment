import json

class TopProduct():
    def __init__(self):
        self.top = {} # pid : (score, status_default)

    def add(self, pid, score, default = False):
        if ((pid not in self.top) or
            (pid in self.top and self.top[pid]['s'] < score) or
            (pid in self.top and self.top[pid]['d'] == True)):
            self.top[pid] = {'s' : score, 'd' : default}

    def getJson(self):
        return json.dumps(self.top)

    def getTop(self, top_n = 5):
        return ','.join(list(map(lambda x : str(x[0]), sorted(self.top.items(), key=lambda x : x[1]['s'], reverse=True)[0:top_n])))