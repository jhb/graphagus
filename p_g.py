from BTrees.IOBTree import IOBTree
from BTrees.OIBTree import OIBTree
from BTrees.IIBTree import IIBTree
from persistent import Persistent
from persistent.mapping import PersistentMapping
from BTrees.Length import Length

class StillConnected(Exception):
    pass

class PObject(Persistent):
    pass


class GraphDB(Persistent):

    def __init__(self):
        
        self.nodes = IOBTree()
        self.edges = IOBTree()
        self.edgedata = IOBTree()

        self.outgoing = IOBTree()
        self.incoming = IOBTree()

        self._name2node=OIBTree()

        self.typeids = PObject()

        self._nodeid = Length(0)
        self._edgeid = Length(0)
        self._typeid = Length(0)

    def nodeid(self):
        self._nodeid.change(1)
        return self._nodeid.value

    def edgeid(self):
        self._edgeid.change(1)
        return self._edgeid.value

    def typeid(self,name):
        if not hasattr(self.typeids,name):            
            self._typeid.change(1)
            setattr(self.typeids,name,self._typeid.value)
        return getattr(self.typeids,name)

    def name2node(self,name):
        return self.lightNode(self._name2node[name])

    def addNode(self,**kwargs):
        id = self.nodeid()
        self.nodes[id]=kwargs
        self._name2node[kwargs['name']]=id #XXX
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

        edge = [start,end,edgetype]
        self.edges[id]=edge
        if kwargs:
            self.edgedata[i]=kwargs
       
        # edgeid:nodeid
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
        out = list(edge)
        out.append(self.edgedata.get(id,{}))
        out.append(id)
        return out

    def delEdge(self,edge):
        if type(edge)==int:
            edge=self.lightEdge(edge)

        start,end,edgetype,props,edgeid = edge

        data = self.outgoing[edgetype][start]
        del(data[edgeid])                
        self.outgoing[edgetype][start]=data
        
        data = self.incoming[edgetype][end]
        del(data[edgeid])                
        self.incoming[edgetype][end]=data

        del(self.edges[edgeid])
        if self.edgedata.has_key(edgeid):
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

    def updateNode(self,lightnode):
        nodeid = lightnode['id']
        data = dict(lightnode)
        self.nodes[nodeid]=data

    def updateEdge(self,lightedge):
        edgeid = lightedge[4]
        edge = list(lightedge[:4])
        data = lightedge[3]
        self.edges[edgeid]=edge
        if data:
            self.edgedata[edgeid]=data
        elif self.edgedata.has_key(edgeid):
            del(self.edgedata[edgeid])

