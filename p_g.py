from BTrees.IOBTree import IOBTree
from BTrees.OIBTree import OIBTree
from BTrees.IIBTree import IIBTree
from persistent import Persistent
from persistent.mapping import PersistentMapping

class GraphDB(Persistent):

    def __init__(self):
        
        self.nodes = IOBTree()
        self.edges = IOBTree()

        self.outgoing = IOBTree()
        self.incoming = IOBTree()

        self._name2node=OIBTree()

        self.typeids = PersistentMapping()

        self.counters=PersistentMapping(dict(nodeid=0,
                                             edgeid=0,
                                             typeid=0))

    def nodeid(self):
        self.counters['nodeid']+=1
        return self.counters['nodeid']

    def edgeid(self):
        self.counters['edgeid']+=1
        return self.counters['edgeid']

    def typeid(self,name):
        if not self.typeids.has_key(name):            
            self.counters['typeid']+=1
            typeids = self.typeids
            typeids[name]=self.counters['typeid']
            self.typeids=typeids

        return self.typeids[name]

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

    def addEdge(self,start,end,edgetype,**kwargs):
        id = self.edgeid()
        typeid = self.typeid(edgetype)
        if type(start) == dict:
            start = start['id']
        if type(end) == dict:
            end = end['id']

        edge = [start,end,typeid,kwargs]
        self.edges[id]=edge
        
        data = self.outgoing.setdefault(typeid,IOBTree()).setdefault(start,{})
        data[id]=end
        self.outgoing[typeid][start]=data

        data = self.incoming.setdefault(typeid,IOBTree()).setdefault(end,{})
        data[id]=start
        self.incoming[typeid][end]=data

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

