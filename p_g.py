from BTrees.IOBTree import IOBTree
from BTrees.OIBTree import OIBTree
from BTrees.IIBTree import IIBTree
from persistent import Persistent
from persistent.mapping import PersistentMapping

class StillConnected(Exception):
    pass

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
    
        if type(edgetype) != int:
            edgetype = self.typeid(edgetype)

        if type(start) == dict:
            start = start['id']
        if type(end) == dict:
            end = end['id']

        edge = [start,end,edgetype,kwargs]
        self.edges[id]=edge
       
        # edgeid:nodeid or nodeid:[edgeid,edgeid]?
        data = self.outgoing.setdefault(edgetype,IOBTree()).setdefault(start,{})
        data[id]=end
        self.outgoing[edgetype][start]=data

        data = self.incoming.setdefault(edgetype,IOBTree()).setdefault(end,{})
        data[id]=start
        self.incoming[edgetype][end]=data

        return self.lightEdge(id,edge)

    def lightEdge(self,id,edge=None):
        if edge==None:
            edge = self.edges[id]
        edge.append(id)
        return edge

    def delEdge(self,edge):
        if type(edge)==int:
            edge=self.edges[edge]

        start,end,edgetype,props,edgeid = edge

        data = self.outgoing[edgetype][start]
        del(data[edgeid])                
        self.outgoing[edgetype][start]=data
        
        data = self.incoming[edgetype][end]
        del(data[edgeid])                
        self.incoming[edgetype][end]=data

        del(self.edges[edgeid])

    def delNode(self,node):
        if type(node)==int:
            node=self.nodes[node]
        nodeid = node['id']
       
        for edgetype in self.outgoing.keys():
            if len(self.outgoing[edgetype].get(nodeid,{}))>0:
                raise StillConnected('outgoing',self.outgoing[edgetype][nodeid])

        for edgetype in self.incoming.keys():
            if len(self.incoming[edgetype].get(nodeid,{}))>0:
                raise StillConnected('incoming',self.incoming[edgetype][nodeid])

        #all good, lets delete
        for edgetype in self.outgoing.keys():
            if self.outgoing[edgetype].has_key(nodeid):
                del(self.outgoing[edgetype][nodeid])
        
        for edgetype in self.incoming.keys():
            if self.incoming[edgetype].has_key(nodeid):
                del(self.incoming[edgetype][nodeid])
        del(self._name2node[node['name']])
        del(self.nodes[nodeid])

    def updateNode(self,node):
        nodeid = node['id']
        data = dict(node)
        self.nodes[nodeid]=data

    def updateEdge(self,edge):
        edgeid = edge[4]
        data = list(edge[:4])
        self.edges[edgeid]=data

