#!/usr/bin/env python3

import inspect, sys, os, csv

class LoaderMeta(type):
    loaders = set()
    def __new__(cls, className, baseClasses, dictOfMethods):
        for basecls in baseClasses:
            if basecls in LoaderMeta.loaders:
                LoaderMeta.loaders.remove(basecls)
        cls = type.__new__(cls, className, baseClasses, dictOfMethods)
        LoaderMeta.loaders.add(cls)
        return cls

class Loader(metaclass=LoaderMeta):
    def __repr__(self):
        if hasattr(self, 'reprs'):
            s = [ "%s=%s" % (t, getattr(self, t)) for t in getattr(self, 'reprs')]
            return '%s(%s)' % (self.__class__.__name__, ','.join(s))
        else:
            return super(Loader, self).__repr__()

    @classmethod
    def load(cls, data_dir):
        # find our file
        fname = os.path.join(data_dir, cls.filename)
        with open(fname) as fd:
            args = inspect.getfullargspec(cls.__init__)
            reqd = args.args[1:-len(args.defaults)] # skip self argument
            opt = args.args[-len(args.defaults):]
            reader = csv.reader(fd)
            header = [t.lower().strip() for t in next(reader)] # some feeds have spaces, etc; normalise somewhat
            # find required headers
            indices = []
            for arg in reqd:
                try:
                    indices.append(header.index(arg))
                except ValueError:
                    raise Exception("required header column %s missing" % arg)
            # find optional headers
            optdict = {}
            for arg in opt:
                try:
                    optdict[arg] = header.index(arg)
                except ValueError:
                    pass
            # create and yield back objects
            for row in reader:
                yield cls(*[row[t] for t in indices], **(dict([(t, row[optdict[t]]) for t in optdict])))

class Agency(Loader):
    filename = "agency.txt"
    reprs = ('id', 'name')
    def __init__(self, agency_name, agency_url, agency_timezone, agency_id=None, agency_lang=None, agency_phone=None, agency_fare_url=None):
        self.id = agency_id or "default"
        self.name = agency_name
        self.url = agency_url
        self.timezone = agency_timezone
        self.lang = agency_lang
        self.phone = agency_phone
        self.fare_url = agency_fare_url

class GTFS:
    def __init__(self, data_dir):
        for cls in LoaderMeta.loaders:
            nm = cls.__name__
            objs = list(cls.load(data_dir))
            setattr(self, nm, objs)

if __name__ == '__main__':
    gtfs = GTFS(sys.argv[1])
    print(gtfs.Agency)


