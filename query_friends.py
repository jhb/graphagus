import p_g, sys, random, time
from ZODB import DB

from ZEO import ClientStorage
from ZODB.FileStorage import FileStorage

size = int(sys.argv[1])
depth = int(sys.argv[2])
runs = int(sys.argv[3])
dbtype = sys.argv[4]


if dbtype.startswith('f'):
    storage = FileStorage('Friends.fs')
else:
    clientid = sys.argv[5]
    addr = ('localhost',1234)
    #storage = ClientStorage.ClientStorage(addr,cache_size=2048*1024*1024,client='shm/p_gclient' +clientid)
    storage = ClientStorage.ClientStorage(addr,cache_size=4096*1024*1024,client='friends' +clientid)
    #storage = ClientStorage.ClientStorage(addr,cache_size=0)
    #storage = ClientStorage.ClientStorage(addr)


db = DB(storage)
connection = db.open()
root = connection.root()
g=root['graphdb']

#maxnum = int(sys.argv[1])
maxnum = 1000
# given a topic, query all authors that like that topic (10 points) or have written an
# article on it (3 points) or have worked on a project on it (5 points)

def addpoints(found,name,points):
    found[name] = found.setdefault(name,0) + points
def doqueries():

    print 'go'
    times = []
    for r in range(1,runs+1):
        print 'run', r,
        
        pid = random.randint(1,size+1)
        #pid = 12345
        pname = 'person%s'%pid
        print pname, 
        
        start = time.time()

        #query code
        person = g.name2node(pname)
        #import ipdb; ipdb.set_trace() 
        startset = set([pid])
        tfriend=g.typeids['friend']

        for d in range(0,depth):
            endset=set()
            for s in startset:
                endset.update(g.outgoing[tfriend][s].values())
            startset = endset
        
        t = time.time()-start
        times.append(t)
        print 'found', len(endset),'in',         
        print t

    print 'average: ', sum(times)/r
    return endset
doqueries()
#conn.close()
#db.close()

#pygraph.connection.close()
