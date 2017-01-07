from core import islisttype
from pprint import pformat

class Traverser(object):
    def __init__(self, graph, paths = None,  store='nodes', output=None,):
        self.g = graph
        if paths:
            if not islisttype(paths):
                paths = list(paths)
        else:
            paths = []
        self.paths = paths
        self.current = 0
        self.store = store
        self.output = output
        self.before = None
        self.after = None
        self.nameddata = []
        self.names = []

    def __iter__(self):
        return iter(self.paths)

    def __repr__(self):
        if self.output is not None:
            return pformat(self.output)
        else:
            return pformat(self.pathendings())

    def t(self,paths,store,output=None):
        t = Traverser(self.g, paths, store,output=output)
        t.before = self
        self.after = t
        return t

    def N(self,ids=None):
        paths = []
        if ids is None:
            paths = [['N',k] for k in self.g.nodes.keys()]
        else:
            if not islisttype(ids):
                ids = [ids]
            for i in ids:
                if i in self.g.nodes:
                    paths.append(['N',i])
                    #out.append(self.g.nodes[i])
        return self.t(list(paths),'nodes')

    def storage(self):
        if self.store == 'nodes':
            storage = self.g.nodes
        else:
            storage = self.g.edgedata
        return storage

    def datastorage(self):
        if self.store == 'nodes':
            return self.g.nodes
        else:
            return self.g.edgedata

    def lightObject(self,id):
        if self.store == 'nodes':
            return self.g.lightNode(id)
        else:
            return self.g.lightEdge(id)


    def values(self, key):
        datastorage = self.datastorage()
        out = []
        for path in self.paths:
            #print path
            data = datastorage[path[-1]]
            if data.has_key(key):
                out.append(data[key])
        return out

    def outE(self,types=None):
        #types = self.g.prepareTypes(types)
        newpaths = []
        for path in self.paths:
            lastnode = path[-1]
            edgeids = self.g.getAllEdges(lastnode,'o',types,1)
            for edgeid in edgeids:
                newpath = list(path)
                newpath.append(edgeid)
                newpaths.append(newpath)
        return self.t(newpaths,'edges')

    def inE(self,types=None):
        #types = self.g.prepareTypes(types)
        newpaths = []
        for path in self.paths:
            lastnode = path[-1]
            edgeids = self.g.getAllEdges(lastnode,'i',types,1)
            for edgeid in edgeids:
                newpath = list(path)
                newpath.append(edgeid)
                newpaths.append(newpath)
        return self.t(newpaths,'edges')

    def pathendings(self,paths=None):
        if paths is None:
            paths = self.paths
        return [path[-1] for path in paths]

    def inN(self):
        newpaths = []
        for path in self.paths:
            newpath = list(path)
            newpath.append(self.g.edges[path[-1]][1])
            newpaths.append(newpath)
        return self.t(newpaths,'nodes')

    def outN(self):
        newpaths = []
        for path in self.paths:
            newpath = list(path)
            newpath.append(self.g.edges[path[-1]][0])
            newpaths.append(newpath)
        return self.t(newpaths,'nodes')

    def out(self,types=None):
         t1 = self.outE(types)
         return t1.inN()

    def _in(self,types=None):
         t1 = self.inE(types)
         return t1.outN()


    @staticmethod
    def gt(value):
        return lambda x: x > value

    def has(self, **kwargs):
        datastorage = self.datastorage()
        newpaths = []
        for path in self.paths:
            bad = 0
            data = datastorage[path[-1]]
            for key,cmp in kwargs.items():
                if callable(cmp):
                    if not cmp(data[key]):
                        bad +=1
                else:
                    if not data[key] == cmp:
                        bad +=1
            if not bad:
                newpaths.append(path)
        return self.t(newpaths,self.store)

    def _as(self,name):
        if type(name)==str:
            self.names.append(name)
        return self

    def select(self,*args):
        out = []
        argparts = [(a,a.split('.')) for a in args]
        argkeys = [p[0] for p in argparts]

        for i in range(0,len(self.paths)):
            t = self
            path = list(reversed(self.paths[i]))
            data = []
            j = 0
            while True:
                names = t.names + ['_id']
                for name in names:
                    for arg,parts in argparts:
                        if parts[0] == name:
                            if len(parts) == 1:
                                val = t.lightObject(path[j])
                            elif parts[1] == '_id':
                                val = path[j]
                            else:
                                val = t.datastorage()[path[j]].get(parts[1],None)
                            data.append((arg,val))
                if t.before is None:
                    break
                else:
                    j += 1
                    t = t.before
            out.append(list(reversed(data)))
        return out

    def pselect(self,*args):
        print pformat(self.select(*args))




T = Traverser


def getTraverser(graph):
    return Traverser(graph)
