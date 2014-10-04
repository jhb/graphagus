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
from ZODB import DB
from ZODB.FileStorage import FileStorage

class StillConnected(Exception):
    pass

class PObject(Persistent):
    pass

#for the node catalog
class Nodegetter():

    def __init__(self,key):
        self.key=key

    def __call__(self,obj,default):
        return obj.get(self.key,default)

#for the edge catalog
class Edgegetter():

    def __init__(self,key):
        self.key=key

    def __call__(self,obj,default):
        return obj[3].get(self.key,default)

class EdgeDict(dict):

    @property
    def i(self):
        return self.get('i',[])

    @property
    def o(self):
        return self.get('o',[])

#higher level API
class Edge(list):

    def __init__(self,g,lightEdge):
        self.g = g
        self.le = lightEdge
        self.extend(lightEdge)

    def __repr__(self):
        return 'Edge(%s)' % self.le

    @property
    def source(self):
        return self.g.node(self[0])

    @property
    def target(self):
        return self.g.node(self[1])

    @property
    def type(self):
        return self.g.revtypes[self[2]]

    @property
    def data(self):
        return self[3]

    def __getattr__(self,key):
        if self.data.has_key(key):
            return self.data[key]
        else:
            raise AttributeError

#higher level API
class Node(dict):

    def __init__(self,g,lightNode):
        self.g = g
        self.ln = lightNode
        self.update(lightNode)
    
    def __repr__(self):
        return 'Node(%s)' % self.ln

    def __getattr__(self,key):
        if self.has_key(key):
            return self[key]
        else:
            raise AttributeError

    def allEdges(self,directions=None,types=None):
        return self.g.getAllEdges(self,directions=directions,types=types)

    def outgoing(self,types=None):
        return self.allEdges('outgoing',types=types)
    o = property(outgoing)

    def incoming(self,types=None):
        return self.allEdges('incoming',types=types)
    i = property(incoming)

class GraphDB(Persistent):

    def __init__(self,nodeindexes=(),edgeindexes=()):
        
        self._init()
        
        self.node_catalog= Catalog()
        self.edge_catalog = Catalog()

    def _init(self):
        self.nodes = IOBTree()
        self.edges = IOBTree()
        self.edgedata = IOBTree()

        self.outgoing = IOBTree()
        self.incoming = IOBTree()

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
            #del(self.edges[edgeid])


    def delNode(self,node):
        if type(node)==int:
            node=self.lightNode(node)
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
    
################## Higher Level API, functionality > speed ###################

    def getAllEdges(self,nodeids,directions=None,types=None):
        

        if type(nodeids) not in (list,tuple):
            nodeids = [nodeids]
        if directions == None:
            directions = ['i','o']
        elif type(directions) not in (list,tuple):
            directions = [directions]

        if types != None:
            if type(types) not in (list,tuple):
                types = [types]
            tmp = []
            for t in types:
                if type(t)==str:
                    t = self.typeid(t)
                tmp.append(t)
            types = tmp
            
        tmp = []
        for n in nodeids:
            if type(n) != int:
                n = n['_id']
            tmp.append(n)
        nodeids = tmp

        out = EdgeDict()

        for direction in directions:
            if direction.startswith('i'):
                d = 'incoming'
            elif direction.startswith('o'):
                d = 'outgoing'
            else:
                raise 'unknown'

            result = []
            container = getattr(self,d)
            
            for edgetype in container.keys():
                if types !=None and edgetype not in types:
                    continue
                for n in nodeids:
                    edges = container[edgetype].get(n,{})
                    for key in edges.keys():
                        result.append(self.edge(key))
            out[direction] = result
        if len(directions) == 1:
            return result
        else:
            return out

    # XXX work in progress
    def getEdges(self,start,end,edgetype):
        #import ipdb; ipdb.set_trace()
        if type(edgetype) != int:
            edgetype = self.typeid(edgetype)

        if type(start) != int:
            start = start['_id']
        if type(end) != int:
            end = end['_id']

        out = []
        targets = self.outgoing.get(edgetype,{}).get(start,{})
        for edgeid,nodeid in targets.items():
            if nodeid==end:
                out.append(self.lightEdge(edgeid))
        return out

    # XXX work in progress
    def addUniqueEdge(self,start,end,edgetype,**kwargs):
        if not self.getEdges(start,end,edgetype):
            return self.addEdge(start,end,edgetype,**kwargs)

    def clean(self):
        #import ipdb; ipdb.set_trace()
        for k in list(self.edges.keys()):
            self.delEdge(k)

        for k in list(self.nodes.keys()):
            self.delNode(k)

        self._init()
    
    def render(self,filename='graphagus',source=False):
        from graphviz import Digraph
        
        dot = Digraph('Graphagus dump',format='svg')
        
        for k in self.nodes.keys():
            n = self.lightNode(k)
            dot.node(str(k),n['name'])
        
        for k in self.edges.keys():
            e = self.lightEdge(k)
            dot.edge(str(e[0]),
                     str(e[1]),
                     self.getType(e))
        if source:
            return dot.source
        else:
            dot.render(filename,cleanup=True)

    def edge(self,lightEdge):
        if type(lightEdge) == int:
            lightEdge = self.lightEdge(lightEdge)
        return Edge(self,lightEdge)

    def node(self,lightNode):
        if type(lightNode) == int:
            lightNode = self.lightNode(lightNode)
        return Node(self,lightNode)
