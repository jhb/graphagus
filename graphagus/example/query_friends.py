import graphagus, sys, random, time
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
    #storage = ClientStorage.ClientStorage(addr,cache_size=2048*1024*1024,client='shm/graphagusclient' +clientid)
    #storage = ClientStorage.ClientStorage(addr,cache_size=4096*1024*1024,client='friends' +clientid)
    #storage = ClientStorage.ClientStorage(addr,cache_size=0)
    storage = ClientStorage.ClientStorage(addr)


db = DB(storage)
connection = db.open()
root = connection.root()
g=root['graphdb']

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
        person = g.queryNode(name=pname)[0]
        #import ipdb; ipdb.set_trace() 
        startset = set([pid])
        tfriend=g.typeid('friend')

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
