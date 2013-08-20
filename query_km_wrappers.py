import p_g, sys, random, time, transaction
from ZODB import DB

from ZEO import ClientStorage
from ZODB.FileStorage import FileStorage

runs = int(sys.argv[1])
dbtype = sys.argv[2]

if dbtype.startswith('f'):
    storage = FileStorage('Data.fs')
else:
    clientid = sys.argv[3]
    addr = ('localhost',1234)
    #storage = ClientStorage.ClientStorage(addr,cache_size=2048*1024*1024,client='shm/p_gclient' +clientid)
    storage = ClientStorage.ClientStorage(addr,cache_size=512*1024*1024,client='querykmcache' +clientid)
    #storage = ClientStorage.ClientStorage(addr,cache_size=0)
    #storage = ClientStorage.ClientStorage(addr)


#db = DB(storage,cache_size=1000000,cache_size_bytes=1024*1024*124)
db = DB(storage,cache_size=200000,cache_size_bytes=1024*1024*124) #this uses 0.03 secs
#db = DB(storage)
connection = db.open()
root = connection.root()
g=root['graphdb']

fixedtarget = None
maxnum = 1000

# given a topic, query all authors that like that topic (10 points) or have written an
# article on it (3 points) or have worked on a project on it (5 points)
def addpoints(found,name,points):
    found[name] = found.setdefault(name,0) + points
queried = {}



def doqueries():

    print 'go'
    times = []
    for r in range(1,runs+1):
        print 'run', r,
        if not fixedtarget:
            tid = random.randint(1,maxnum)
            tn = 'topic%s' % tid
        else:
            tn = fixedtarget
        print '%-9s' %tn,

        queried[tn] = queried.setdefault(tn,0)+1
        
        start = time.time()
        
        #deprecated!
        #connection.sync()

        transaction.begin()
        g = root['graphdb']
        
        #topic = g.name2node(tn)
        topic = g.getFullNode(name=tn)[0]
        found = dict()
        #import ipdb; ipdb.set_trace()

        for node in topic.incomingN('topic').values():
            label = node.label
            if label == 'person':
                addpoints(found,node.id,10)
            elif label == 'article':
                for nodeid2 in node.outgoing('author').values():
                    addpoints(found,nodeid2,5)
 
            elif label == 'project':
                for nodeid2 in node.outgoing('member').values():
                    addpoints(found,nodeid2,3)
    
        
        t = time.time()-start
        times.append(t)
        print 'found', len(found),'in',  
        print t

    print 'average: ', sum(times)/r
    return found,queried
f,q=doqueries()
runs = 1
#conn.close()
#db.close()

#pygraph.connection.close()
