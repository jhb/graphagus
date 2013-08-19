from BTrees.IOBTree import IOBTree
from BTrees.OIBTree import OIBTree
from BTrees.IIBTree import IIBTree
from persistent import Persistent
from persistent.mapping import PersistentMapping
from BTrees.Length import Length
from repoze.catalog.catalog import Catalog
from repoze.catalog.indexes.field import CatalogFieldIndex
from repoze.catalog.indexes.text import CatalogTextIndex
from repoze.catalog.indexes.keyword import CatalogKeywordIndex
from repoze.catalog.indexes.path import CatalogPathIndex
from repoze.catalog import query as rc_query


class StillConnected(Exception):
    pass

class PObject(Persistent):
    pass

class get_key():

    def __init__(self,key):
        self.key=key

    def __call__(self,object,default):
        return object.get(self.key,default)


class GraphDB(Persistent):

    def __init__(self,nodeindexes=(),edgeindexes=()):
        
        self.nodes = IOBTree()
        self.edges = IOBTree()
        self.edgedata = IOBTree()

        self.outgoing = IOBTree()
        self.incoming = IOBTree()

        self.typeids = PObject()

        self._nodeid = Length(0)
        self._edgeid = Length(0)
        self._typeid = Length(0)

        self.node_catalog= Catalog()
        self.edge_catalog = Catalog()


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

    def addNode(self,**kwargs):
        id = self.nodeid()
        self.nodes[id]=kwargs
        ln =  self.lightNode(id,kwargs)
        self.node_catalog.index_doc(id,ln)
        return ln
    
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

        le =  self.lightEdge(id,edge)
        self.edge_catalog.index_doc(id,le)
        return le

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

        self.edge_catalog.unindex_doc(edgeid)
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

        self.node_catalog.unindex_doc(nodeid)                
        del(self.nodes[nodeid])

    def updateNode(self,lightnode):
        nodeid = lightnode['id']
        data = dict(lightnode)
        self.nodes[nodeid]=data
        self.node_catalog.reindex_doc(nodeid,lightnode)

    def updateEdge(self,lightedge):
        edgeid = lightedge[4]
        edge = list(lightedge[:4])
        data = lightedge[3]
        self.edges[edgeid]=edge
        if data:
            self.edgedata[edgeid]=data
        elif self.edgedata.has_key(edgeid):
            del(self.edgedata[edgeid])
        self.edge_catalog.reindex_doc(edgeid,lightedge)            


    def queryNode(self,**kwargs):
        #only one at the moment
        key,value = kwargs.items()[0]
        result = self.node_catalog.query(rc_query.Eq(key,value))
        return [self.lightNode(i) for i in result[1]]
