import cPickle, random, sys

if len(sys.argv) < 2:
    print 'usage: %s number_of_people' % sys.argv[0]
    sys.exit()

num_people = int(sys.argv[1])
num = 50

friendids = range(1,num_people+1)
friends={}
for i in friendids:
    while 1:
        sample = random.sample(friendids,num)
        if i not in sample:
            break
    friends[i]=sample
    if i % 10000 == 0:
        print i
cPickle.dump(friends,open('friends.pickle','w'))
    
