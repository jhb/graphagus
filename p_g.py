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
            self.edge_catalog.index_doc(id,le)
       
        # edgeid:nodeid
        data = self.outgoing.setdefault(edgetype,IOBTree()).setdefault(start,{})
        data[id]=end
        self.outgoing[edgetype][start]=data

        data = self.incoming.setdefault(edgetype,IOBTree()).setdefault(end,{})
        data[id]=start
        self.incoming[edgetype][end]=data

        le =  self.lightEdge(id,edge)
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

        del(self.edges[edgeid])
        if self.edgedata.has_key(edgeid):
            self.edge_catalog.unindex_doc(edgeid)
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

    def pq(self,**kwargs):
        return PathQuery(self,**kwargs)


    def manage_fixupOwnershipAfterAdd(self,*args,**kwargs):
        return

class PathQuery(object):

    def __init__(self,graph,**kwargs):

        self.graph=graph
        self.paths=[]
        self.pathtypes=[] #0:node, 1:edge
        if kwargs:
            nodeids = self.graph.node_catalog.query(self.graph.kwQuery(**kwargs))[1]
            for id in nodeids:
                self.paths.append([id])
            self.pathtypes=[0]                

        self.outkeys = list()
        self.inkeys = list(self.graph.incoming.keys())

    def __len__(self):
        return len(self.paths)

    def __getitem__(self,i):
        return self.paths[i]

    def getLast(self):
        return [p[-1] for p in self.paths]

    last = property(getLast)

    def nextHop(self,direction,*args):
        #import ipdb; ipdb.set_trace()
        edgetypes = [getattr(self.graph.typeids,et) for et in args]

        out = []
        main = getattr(self.graph,direction)
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
                                out.append(tmp)
        new = PathQuery(self.graph)
        new.paths = out
        new.pathtypes = list(self.pathtypes)
        new.pathtypes.extend([1,0])
        return new

    def o(self,*args):
        return self.nextHop('outgoing',*args)

    def i(self,*args):
        return self.nextHop('incoming',*args)

    @property
    def N(self):
        out = []
        for p in self.paths:
            node = self.graph.lightNode(p[-1])
            out.append(node)
        return out           

    @property
    def E(self):
        out = []
        for p in self.paths:
            node = self.graph.lightEdge(p[-2])
            out.append(node)
        return out           

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

    @property
    def P(self):
        return [self.expand(p) for p in self.paths]

        
