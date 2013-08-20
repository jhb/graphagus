import p_g

def mergedict(d):
    out = {}
    for k,v in d.items():
        out.update(v)
    return out        


class Node(object):

    def __init__(self,graph,context):
        self.g = graph
        t = type(context)
        if t==int:
            self.context = self.g.lightNode(context)
        elif t==dict:
            self.context = context
        else:
            raise 'initdata "%s" is of type "%s"' % (initdata,t)

    def nexthop(self,direction,*args,**kwargs):
        revtypes = self.g.revtypes
        allowed = [getattr(self.g.typeids,a) for a in args]
        out = {}
        main = getattr(self.g,direction)

        for key,idx in main.items():
            if not allowed or key in allowed:
                out[revtypes[key]]=main[key].get(self.id,{})


        if kwargs.get('merge',False):
            return mergedict(out)
        else:
            return out

    def nexthopN(self,direction,*args,**kwargs):
        data = self.nexthop(direction,*args)
        out = {}
        for k,v in data.items():
            out[k]={}
            for edgeid,nodeid in v.items():
                out[k][edgeid]=self.g.getFullNode(nodeid)
        if kwargs.get('merge',False):
            return mergedict(out)
        else:
            return out

    def outgoing(self,*args,**kwargs):
        if len(args)==1: kwargs['merge']=1
        return self.nexthop('outgoing',*args,**kwargs)

    def outgoingN(self,*args,**kwargs):
        if len(args)==1: kwargs['merge']=1
        return self.nexthopN('outgoing',*args,**kwargs)

    def incoming(self,*args,**kwargs):
        if len(args)==1: kwargs['merge']=1
        return self.nexthop('incoming',*args,**kwargs)

    def incomingN(self,*args,**kwargs):
        if len(args)==1: kwargs['merge']=1
        return self.nexthopN('incoming',*args,**kwargs)

  
    def __getattr__(self,name):
        try:
            return self.context.get(name)
        except KeyError:
            raise AttributeError

    def __repr__(self):
        return '<Node(%s) at %s>' % (self.context,hex(id(self)))
