import cPickle, random, sys

if len(sys.argv) < 2:
    print 'usage: %s number_of_people' % sys.argv[0]
    sys.exit()

num_people = int(sys.argv[1])
num_projects = num_people / 5
num_topics = 1000

data = {}

people = {}
articles = {}
projects = {}

a = 1

topics = ['topic%s' % i for i in range(1,num_topics+1)]
for i in range(1,num_people+1):
    name = 'person%s' % i
    if not i % 1000:
        print 'people: %s' % i
    num_main_topics = random.randint(2,8) 
    main_topics = random.sample(topics,num_main_topics)
    people[name]=list(main_topics)
    num_articles = random.randint(1,20)
    for j in range(1,num_articles+1):
        aname = 'article%s' % a
        a += 1
        atopics = set(random.sample(main_topics,2))
        while len(atopics) < random.randint(3,8):
            atopics.update(random.sample(topics,1))
        articles[aname] = dict(topics=list(atopics),
                               author=name)

peoplenames = people.keys()

for i in range(1,num_projects+1):
    projectname = 'project%s' % i
    if not i % 10:
        print 'projects: %s' % i
    projectdata = {}
    membernames = random.sample(peoplenames,random.randint(3,30))
    membertopics = set()
    for membername in membernames:
        membertopics.update(people[membername])
    projecttopics = set(random.sample(membertopics,2))
    while len(projecttopics) < random.randint(3,8):
        projecttopics.update(random.sample(topics,1))
    projects[projectname] = dict(members=membernames,
                                 topics = list(projecttopics))
    
data = dict(topics=topics,
            people=people,
            articles=articles,
            projects=projects,)

cPickle.dump(data,open('kmdata.pickle','w'))




    
