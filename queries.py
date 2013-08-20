import random
import time
import transaction

runs = 1
fixedtarget = None
maxnum=1000
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
        topic = g.queryNode(name=tn)[0]
        found = dict()
        #import ipdb; ipdb.set_trace()

        etopic = g.typeids.topic
        eauthor = g.typeids.author
        emember = g.typeids.member

        for nodeid1 in g.incoming[etopic][topic['id']].values():
            node1 = g.nodes[nodeid1]
            label = node1['label']

            if label=='person':
                addpoints(found,nodeid1,10)
            
            elif label == 'article':
                for nodeid2 in g.outgoing[eauthor][nodeid1].values():
                    addpoints(found,nodeid2,5)
 
            elif label == 'project':
                for nodeid2 in g.outgoing[emember][nodeid1].values():
                    addpoints(found,nodeid2,3)
    
        
        t = time.time()-start
        times.append(t)
        print 'found', len(found),'in',  
        print t

    print 'average: ', sum(times)/r
    return found,queried
#conn.close()
#db.close()

#pygraph.connection.close()
