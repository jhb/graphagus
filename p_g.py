from BTrees.IOBTree import IOBTree
from BTrees.OIBTree import OIBTree
from BTrees.IIBTree import IIBTree
from persistent import Persistent

class GraphDB(Persistent):

    def __init__(self):
        
        self.nodes = IOBTree()
        self.edges = IOBTree()

        self.outgoing = IOBTree()
        self.incoming = IOBTree()

        self._name2node=OIBTree()

        self.typeids = {}

        self._nodeid = 0
        self._edgeid = 0
        self._typeid = 0

    def nodeid(self):
        self._nodeid+=1
        return self._nodeid

    def edgeid(self):
        self._edgeid+=1
        return self._edgeid

    def typeid(self,name):
        if self.typeids.has_key(name):            
            return self.typeids[name]
        else:            
            self._typeid+=1
            self.typeids[name]=self._typeid
            return self._typeid

    def name2node(self,name):
        return self.lightNode(self._name2node[name])

    def addNode(self,**kwargs):
        id = self.nodeid()
        self.nodes[id]=kwargs
        self._name2node[kwargs['name']]=id
        return self.lightNode(id,kwargs)
    
    def lightNode(self,id,node=None):
        if node==None:
            node = self.nodes[id]
        out = dict(node)
        out['id'] = id
        return out

    def addEdge(self,start,end,type,**kwargs):
        id = self.edgeid()
        typeid = self.typeid(type)
        startid = start['id']
        endid = end['id']

        edge = [start['id'],end['id'],typeid,kwargs]
        self.edges[id]=edge
        
        data = self.outgoing.setdefault(typeid,IOBTree()).setdefault(startid,{})
        data[id]=endid
        self.outgoing[typeid][startid]=data

        data = self.incoming.setdefault(typeid,IOBTree()).setdefault(endid,{})
        data[id]=startid
        self.incoming[typeid][endid]=data

        return self.lightEdge(id,edge)

    def lightEdge(self,id,edge=None):
        if edge==None:
            edge = self.edges[id]
        out = [id]
        out.extend(edge)
        return out

####### not used yet ############

    def getOutgoing(self,typeid,nodeid):
        data = self.outgoing[typeid].setdefault(nodeid,{})
        out = []
        for eid,nid in data.keys():
            out.append(self.lightEdge(eid),self.lightNode(nid))
        return out
    
    def getIncoming(self,typeid,nodeid):
        data = self.outgoing[typeid].setdefault(nodeid,{})
        out = []
        for eid,nid in data.keys():
            out.append(self.lightEdge(eid),self.lightNode(nid))
        return out

