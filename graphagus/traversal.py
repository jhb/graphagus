from itertools import chain



class Traverser(object):

    def __init__(self,graph,input=None,store='nodes'):
        self.g=graph
        self.input = input and iter(input) or iter([])
        self.output = None
        self.current = 0
        self.store = store



    def __iter__(self):
        return self.input

    def __repr__(self):
        return repr(list(self.input))

    def t(self,data,store):
        return Traverser(self.g,input=data,store=store)

    def N(self,id=None):
        if id != None:
            if id in self.g.nodes.keys():
                data = [id]
            else:
                data = []
        else:
            data = self.g.nodes.keys()

        return self.t(data,'nodes')

    def storage(self):
        if self.store == 'nodes':
            storage = self.g.nodes
        else:
            storage = self.g.edgedata
        return storage

    def values(self,key):
        s = self.storage()

        def gendata():
            for i in self.input:
                yield(s[i][key])
        return self.t(gendata(),self.store)

    def outE(self,types=None):
        data = self.g.getAllEdges(self.input,'o',types,1)
        return self.t(data,'edges')

    def out(self,types=None):
        data = [e[1] for e in self.g.getAllEdges(self.input, 'o', types, 0)]
        return self.t(data,'nodes')

    def inN(self):
        return self.t([self.g.edges[i][1] for i in self.input],'nodes')

    def inE(self,types=None):
        data = self.g.getAllEdges(self.input,'i',types,1)
        return self.t(data,'edges')

    def _in(self,types=None):
        data = [e[0] for e in self.g.getAllEdges(self.input, 'i', types, 0)]
        return self.t(data,'nodes')

    def outN(self):
        return self.t([self.g.edges[i][0] for i in self.input],'nodes')

    def has(self,**kwargs):
        s = self.storage()

        def gendata(kwargs):
            for i in self.input:
                bad = 0;
                props = s[i]
                for key,cmp in kwargs.items():
                    if callable(cmp):
                        if not cmp(props[key]):
                            bad +=1
                    else:
                        if not props[key] == cmp:
                            bad +=1
                if not bad:
                    yield i

        return self.t(gendata(kwargs),self.store)


    def gt(self,value):
        return lambda x: x > value

T = Traverser

def getTraverser(graph):
    return Traverser(graph)