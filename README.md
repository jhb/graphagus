p_g
===

A simple (but fast) graph database for python using zodb

The main file is p_g.py. A little testsetup is included. The testsetup consists of
- 1k topic nodes
- 100k person nodes, with 2-8 random 'topic' edges to topics
- ~1m articles: for each user 1-20 article nodes are generated
  - each article has 3-8 'topic' edges to topics
  - each article has one 'author' edge to a person
- 20k projects
  - 3-30 'member' edges to person nodes
  - 3-8 'topic' edges to topics

Ok, lets setup the server side:

    cd p_g

    #setup
    virtualenv --no-site-packages .
    source bin/activate
    pip install ZODB3

    #prepare some sample data. Output: kmdata.pickle
    python kmdata.py 100000

    #read the data into the zodb, creating Data.fs
    python import_kmdata.py kmdata.pickle

    #fire up the zeoserver
    runzeo -a localhost:1234 -f Data.fs

Now, in a seperate console (keep the zeoserver running)

    #pack the Data.fs, only needed for the first time
    #reduces the Data.fs to ~400MB

    cd p_g
    source bin/activate
    zeopack localhost:1234


The real query. Given a random chosen topic, calculate for a person:

- 10 points for being directly connected to the topic
-  5 points for being author of an article connected to the topic
-  3 points for being a member of a project connected to the topic

    #Query the database 201 times using zeo, as client1
    python query_km.py 201 z 1
    
    #on my machine - average:  0.0951218937167
    #this can be improved by doing it again:

    python query_km.py 201 z 1
    #on my machine - average:  0.0291442408491
    

