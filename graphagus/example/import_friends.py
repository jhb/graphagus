import cPickle, graphagus, time, sys
import transaction
from ZODB.FileStorage import FileStorage
from ZODB.DB import DB
from BTrees.IOBTree import IOBTree
from ZODB.Connection import resetCaches

#this is absolutely ugly, will be modified back to a sane
#approach
g = None
root = None
connection = None
db = None
storage = None

def doopen():
    print 'open',
    global g, root, connection,db,storage
    storage = FileStorage('Friends.fs')
    db = DB(storage, cache_size=100,cache_size_bytes=0)
    connection = db.open()
    root = connection.root()
    if not root.has_key('graphdb'):
        print 'creating'
        root['graphdb']=graphagus.GraphDB()
    g=root['graphdb']

def doclose():
    transaction.commit()
    resetCaches()
    connection.close()
    db.close()
    storage.close()
doopen()

if len(sys.argv) < 2:
    filename = 'friends.pickle'
else:
    filename = sys.argv[1]

print 'deleting'
root['graphdb']=graphagus.GraphDB()
g=root['graphdb']
g.node_catalog['name']=graphagus.CatalogFieldIndex(graphagus.Nodegetter('name'))

print 'reading friends'
f = open(filename)
friends = cPickle.load(f)
f.close()

print 'adding nodes'
nids = {}
g.debug=0

size = 10000

for a,targets in friends.iteritems():
    name = 'person%s' % a
    node = g.addNode(name=name)
    nids[a]=node['_id']
    if not a % size:
        transaction.commit()
        resetCaches()
        print 'node', a
transaction.commit()

print 'adding relations'
i = 0
edges=[]
incoming={}
outgoing={}
typeid = g.typeid('friend')
for a,targets in friends.iteritems():
    for b in targets:
        i+=1
        aid=nids[a]
        bid=nids[b]
        g.edges[i]=[aid,bid,typeid,{}]
        outgoing.setdefault(aid,[]).append((i,bid))
        incoming.setdefault(bid,[]).append((i,aid))
        #outgoing.setdefault(aid,{})[i]=bid
        #outgoing.setdefault(bid,{})[i]=aid
        #g.addEdge(nids[a],nids[b],'friend')
        if not i%(size/10):
            transaction.savepoint(1)
        if not i%size:
            transaction.commit()
            resetCaches()
            #doclose()
            #doopen()

            print 'edge', i
g._edgeid=graphagus.Length(i)
transaction.commit()
del(friends)
print len(outgoing),len(incoming)


print 'writing outgoing'
g.outgoing[typeid]=IOBTree()
i=0
#for k,edgeids in outgoing.iteritems():
for start,endtuples in outgoing.iteritems():
    i+=1
    g.outgoing[typeid][start]=dict(endtuples)
    if not i%(size/100):
        print 'out', i
        transaction.savepoint(1)
    if not i%(size/10):
        transaction.commit()
        resetCaches()
        print 'out commited'
transaction.commit()
del(outgoing)

#import ipdb; ipdb.set_trace()
print 'writing incoming'
g.incoming[typeid]=IOBTree()
i=0
#for k,edgeids in incoming.iteritems():
for end,starttuples in incoming.iteritems():
    i+=1
    g.incoming[typeid][end]=dict(starttuples)
    if not i%(size/100):
        print 'in', i
        transaction.savepoint(1)
    if not i%(size/10):
        transaction.commit()
        resetCaches()
        print 'in commited'
transaction.commit()
del(incoming)





doclose()
print 'fini'        
