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

class Nodegetter():

    def __init__(self,key):
        self.key=key

    def __call__(self,object,default):
        return object.get(self.key,default)

class Edgegetter():

    def __init__(self,key):
        self.key=key

    def __call__(self,obj,default):
        return obj[3].get(self.key,default)



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
            self.revtypes[self._typeid.value]=name
        return getattr(self.typeids,name)

    @property
    def revtypes(self):
        if not hasattr(self,'_v_revtypes'):
            dir(self.typeids)
            dir(self.typeids)
            self._v_revtypes = dict([(v,k) for k,v in self.typeids.__dict__.items()])
        return self._v_revtypes

    def getType(self,typeid):
        if type(typeid) != int:
            #lets assume an edge
            typeid = typeid[2]
        return self.revtypes[typeid]


    def addNode(self,**kwargs):
        _id = self.nodeid()
        self.nodes[_id]=kwargs
        ln =  self.lightNode(_id,kwargs)
        self.node_catalog.index_doc(_id,ln)
        return ln
    
    def lightNode(self,_id,node=None):
        "{'id':nodeid, ...other attributes...}"
        if node==None:
            node = self.nodes[_id]
        out = dict(node)
        out['_id'] = _id
        return out

    def addEdge(self,start,end,edgetype,**kwargs):
        _id = self.edgeid()
    
        if type(edgetype) != int:
            edgetype = self.typeid(edgetype)

        if type(start) == dict:
            start = start['_id']
        if type(end) == dict:
            end = end['_id']

        edge = [start,end,edgetype]
        self.edges[_id]=edge
        
        if kwargs:
            self.edgedata[_id]=kwargs
            le =  self.lightEdge(_id,edge)
            self.edge_catalog.index_doc(_id,le)
       
        le =  self.lightEdge(_id,edge)
        # edgeid:nodeid
        data = self.outgoing.setdefault(edgetype,IOBTree()).setdefault(start,{})
        data[_id]=end
        self.outgoing[edgetype][start]=data

        data = self.incoming.setdefault(edgetype,IOBTree()).setdefault(end,{})
        data[_id]=start
        self.incoming[edgetype][end]=data

        return le

    def lightEdge(self,_id,edge=None):
        '[sourceid targetid typeid kwargs edgeid]'
        if edge==None:
            edge = self.edges[_id]
        out = list(edge)
        out.append(self.edgedata.get(_id,{}))
        out.append(_id)
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
            self.edge_catalog.unindex_doc(edgeid)
            del(self.edges[edgeid])


    def delNode(self,node):
        if type(node)==int:
            node=self.nodes[node]
        nodeid = node['_id']
       
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
        nodeid = lightnode['_id']
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
            self.edge_catalog.reindex_doc(edgeid,lightedge)            
        elif self.edgedata.has_key(edgeid):
            del(self.edgedata[edgeid])
            self.edge_catalog.unindex_doc(edgeid)

    def kwQuery(self,**kwargs):
        kwitems = kwargs.items()
        key,value = kwitems[0]
        query = rc_query.Eq(key,value) 
        for k,v in kwitems[1:]:
            query = query & rc_query.Eq(k,v)
        return query 

    def queryNode(self,**kwargs):
        result = self.node_catalog.query(self.kwQuery(**kwargs))
        return [self.lightNode(i) for i in result[1]]
        
    def queryEdge(self,**kwargs):
        result = self.edge_catalog.query(self.kwQuery(**kwargs))
        return [self.lightEdge(i) for i in result[1]]

    def getAllEdges(self,direction,nodeid):
        if type(nodeid) != int:
            nodeid = nodeid['_id']
        out = []
        container = getattr(self,direction)
        for edgetype in container.keys():
            d = container[edgetype].get(nodeid,{})
            for key in d.keys():
                out.append(self.lightEdge(key))
        return out

    def getEdges(self,start,end,edgetype,**kwargs):

        if type(edgetype) != int:
            edgetype = self.typeid(edgetype)

        if type(start) == dict:
            start = start['_id']
        if type(end) == dict:
            end = end['_id']

        out = []
        targets = self.outgoing.get(edgetype,{}).get(start,{})
        for edgeid,nodeid in targets.items():
            if nodeid==end:
                out.append(self.lightEdge(edgeid))
        return out

    def addUniqueEdge(self,start,end,edgetype,**kwargs):
        if not self.getEdges(start,end,edgetype):
            return self.addEdge(start,end,edgetype,**kwargs)
