from UserList import UserList

class Path(UserList):
    
    def __init__(self,pathquery,initlist):
        super(Path,self).__init__(initlist)
        self.pathquery = pathquery

    def expand(self):
        return self.pathquery.expand(self)

    def nodes(self):
        return [self.pathquery.graph.lightNode(nid) for nid in self[::2]]

    def edges(self):
        return [self.pathquery.graph.lightEdge(eid) for eid in self[1::2]]


class PathQuery(object):

    def __init__(self,graph,*args,**kwargs):

        self.graph=graph
        self.paths=[]
        self.pathtypes=[] #0:node, 1:edge
        
        if args:
            if type(args[0]) == int:
                self.paths.append(Path(self,args))
            else:
                for id in args[0]:
                    self.paths.append(Path(self,[id]))
            self.pathtypes=[0]                
        
        elif kwargs:
            nodeids = self.graph.node_catalog.query(self.graph.kwQuery(**kwargs))[1]
            for id in nodeids:
                self.paths.append(Path(self,[id]))
            self.pathtypes=[0]                

        self.outkeys = list()
        self.inkeys = list(self.graph.incoming.keys())

    def __len__(self):
        return len(self.paths)

    def __getitem__(self,i):
        return self.paths[i]

    def lastNodeIds(self):
        return [p[-1] for p in self.paths]

    nodeids = property(lastNodeIds)

    def nextHop(self,direction,*args):
        #import ipdb; ipdb.set_trace()
        edgetypes = [getattr(self.graph.typeids,et) for et in args]

        out = []
        main = getattr(self.graph,direction)
        
        new = PathQuery(self.graph)
        new.pathtypes = list(self.pathtypes)
        
        for key in main.keys():
            if not edgetypes or key in edgetypes:                    
                idx = main[key]
                for path in self.paths:
                    last = path[-1]
                    others = idx.get(last,None)
                    if others:
                        for k,v in others.items():
                            if k not in path[1::2]:
                                tmp = list(path)
                                tmp.append(k)
                                tmp.append(v)
                                out.append(Path(new,tmp))
        new.paths = out
        new.pathtypes.extend([1,0])

        return new

    def o(self,*args):
        return self.nextHop('outgoing',*args)

    def i(self,*args):
        return self.nextHop('incoming',*args)

    def nodes(self):
        out = []
        for p in self.paths:
            node = self.graph.lightNode(p[-1])
            out.append(node)
        return out           

    def edges(self):
        out = []
        for p in self.paths:
            node = self.graph.lightEdge(p[-2])
            out.append(node)
        return out           

    @property
    def P(self):
        return [p.expand() for p in self.paths]

    def expand(self,path):
        if type(path)==int:
            path = self.paths[path]
        out = []
        i = 0
        for t in self.pathtypes:
            if t==0:
                out.append(self.graph.lightNode(path[i]))
            elif t==1:
                out.append(self.graph.lightEdge(path[i]))
            i+=1
        return out
    
    def distinct(self):
        return PathQuery(self.graph,set(self.nodeids))
        
