import p_g, sys, random, time,transaction
from ZODB import DB

from ZEO import ClientStorage
from ZODB.FileStorage import FileStorage

dbtype = sys.argv[1]
if dbtype.startswith('f'):
    if len(sys.argv)>2:
        filename=sys.argv[2]
    else:
        filename='Test.fs'
    storage = FileStorage(filename)
else:
    if len(sys.argv)>2:
        clientid=sys.argv[2]
    else:
        clientid='1'
    addr = ('localhost',1234)        
    storage = ClientStorage.ClientStorage(addr,cache_size=1024*1024*1024,client='testcache' +clientid)   
db = DB(storage)
connection = db.open()
root = connection.root()

if 1 or not root.has_key('graphdb'):
    root['graphdb']=p_g.GraphDB()
g=root['graphdb']

g.node_catalog['name']=p_g.CatalogFieldIndex(p_g.get_key('name'))

likes = g.typeid('likes')

alice = g.addNode(name='alice')
bob = g.addNode(name='bob')

g.addEdge(alice,bob,likes)

charlie = g.addNode(name='charlie')

try:
    g.delNode(bob)
except p_g.StillConnected, e:
    print "can't delete bob, connected to ", e.args

#import ipdb; ipdb.set_trace()

g.delEdge(e.args[1].keys()[0])
g.delNode(bob)

g.addEdge(alice,charlie,likes)

edges = g.incoming[likes][charlie['id']].keys() 
edge = g.lightEdge(edges[0])#cheated
edge[3]['intention']='serious4'
g.updateEdge(edge)

alice['status']='engaged'
g.updateNode(alice)

print 'Alice got found:',g.queryNode(name='alice')[0]==alice
print 'ok, all good'
transaction.commit()


