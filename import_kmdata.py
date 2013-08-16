import cPickle, time, uuid, sys
import p_g,transaction
from ZODB.FileStorage import FileStorage
from ZODB.DB import DB

storage = FileStorage('Data.fs')
db = DB(storage)
connection = db.open()
root = connection.root()

if len(sys.argv) < 2:
    filename = 'kmdata_100k.pickle'
else:
    filename = sys.argv[1]

print 'deleting'
root['graphdb']=p_g.GraphDB()
g=root['graphdb']

print 'reading'
f = open(filename)
kmdata = cPickle.load(f)
f.close()

print 'topics'
i = 0
for name in kmdata['topics']:
    i+=1
    g.addNode(name=name,label='topic')
    if not i % 1000:
        print 'topic: ',i

print 'people'
i = 0
for name,data in kmdata['people'].items():
    i+=1
    person = g.addNode(name=name,label='person')
    for topicname in data:
        topic = g.name2node(topicname)
        g.addEdge(person,topic,'topic')
    if not i % 1000:
        print 'person: ',i
        transaction.savepoint()
    if not i %50000:
        transaction.commit()

print 'projects'
i = 0
for name,data in kmdata['projects'].items():
    i+=1
    project = g.addNode(name=name,label='project')
    for topicname in data['topics']:
        topic = g.name2node(topicname)
        g.addEdge(project,topic,'topic')
    for membername in data['members']:
        member = g.name2node(membername)
        g.addEdge(project,member,'member')
    if not i % 1000:
        print 'project: ',i
        transaction.savepoint()
    if not i %50000:
        transaction.commit()

print 'articles'
i = 0
for name,data in kmdata['articles'].items():
    i+=1
    article = g.addNode(name=name,label='article')
    for topicname in data['topics']:
        topic = g.name2node(topicname)
        g.addEdge(article,topic,'topic')
    author = g.name2node(data['author'])
    g.addEdge(article,author,'author')
    if not i % 1000:
        print 'article: ',i
        transaction.savepoint()
    if not i %50000:
        transaction.commit()

transaction.commit()
connection.close()
db.close()

