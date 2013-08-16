import p_g, sys, random, time,transaction
from ZODB import DB

from ZEO import ClientStorage
from ZODB.FileStorage import FileStorage

dbtype = sys.argv[1]
if dbtype.startswith('f'):
    if len(sys.argv)>2:
        filename=sys.argv[2]
    else:
        filename='Data.fs'
    storage = FileStorage(filename)
else:
    if len(sys.argv)>2:
        clientid=sys.argv[2]
    else:
        clientid='1'
    addr = ('localhost',1234)        
    storage = ClientStorage.ClientStorage(addr,cache_size=1024*1024*1024,client='console_cache' +clientid)   
db = DB(storage)
connection = db.open()
root = connection.root()
try:
    g=root['graphdb']
except:
    pass
