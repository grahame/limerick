#!/usr/bin/env python3

import inspect, sys, os, csv, datetime, collections

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
            args = inspect.getfullargspec(cls.create)
            ndefs = len(args.defaults or [])
            assert(args.args[0] == 'cls')
            assert(args.args[1] == 'res')
            reqd = args.args[2:-ndefs] # skip self argument
            opt = args.args[-ndefs:]
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
            res = {}
            fields = named_tuple = None
            for row in reader:
                cls.create(res, *[row[t] for t in indices], **(dict([(t, row[optdict[t]]) for t in optdict])))
                if fields is None:
                    fields = list(res.keys())
                    fields.sort()
                    named_tuple = collections.namedtuple(cls.__name__+"_", fields)
                yield named_tuple(*[res[t] for t in fields])

class Agency(Loader):
    filename = "agency.txt"
    reprs = ('agency_id', 'name')
    @classmethod
    def create(cls, res, agency_name, agency_url, agency_timezone, agency_id=None, agency_lang=None, agency_phone=None, agency_fare_url=None):
        res['agency_id'] = agency_id or "default"
        res['name'] = agency_name
        res['url'] = agency_url
        res['timezone'] = agency_timezone
        res['lang'] = agency_lang
        res['phone'] = agency_phone
        res['fare_url'] = agency_fare_url

def mk_enum(type_name, *args):
    names = []
    for arg in args:
        success = False
        for i in range(1, len(arg)):
            short = arg[:i]
            if short not in names:
                names.append(short)
                success = True
                break
        if not success:
            raise Exception("non-unique name in enum")
    enum = type(type_name, (), dict(zip(args, names)))
    enum.names = args
    enum.values = names
    return enum

def parse_gtfs_date(s):
    return datetime.datetime.strptime(s, "%Y%m%d").date()

class Stop(Loader):
    filename = "stops.txt"
    reprs = ('stop_id', 'name')
    LocationType = mk_enum('LocationType', 'stop', 'station')
    WheelchairBoarding = mk_enum('WheelchairBoarding', 'unknown', 'possibly', 'none')
    @classmethod
    def create(cls, res, stop_id, stop_name, stop_lat, stop_lon, stop_code=None, stop_desc=None, zone_id=None, stop_url=None, location_type=None, parent_station=None, stop_timezone=None, wheelchair_boarding=None):
        res['stop_id'] = stop_id
        res['name'] = stop_name
        res['latlng'] = map(float, (stop_lat, stop_lon))
        res['code'] = stop_code
        res['desc'] = stop_desc
        res['zone_id'] = zone_id
        res['url'] = stop_url
        if location_type == '1':
            res['location_type']= Stop.LocationType.station
        else:
            res['location_type']= Stop.LocationType.stop
        res['parent_station'] = parent_station
        res['timezone'] = stop_timezone
        if wheelchair_boarding == '1':
            res['wheelchair_boarding'] = Stop.WheelchairBoarding.possibly
        elif wheelchair_boarding == '2':
            res['wheelchair_boarding'] = Stop.WheelchairBoarding.none
        else:
            res['wheelchair_boarding'] = Stop.WheelchairBoarding.unknown

class Route(Loader):
    filename = "routes.txt"
    RouteType = mk_enum('RouteType', 'tram', 'subway', 'rail', 'bus', 'ferry', 'cablecar', 'gondola', 'funicular')
    reprs = ('route_id', 'short_name')
    @classmethod
    def create(cls, res, route_id, route_short_name, route_long_name, route_type, agency_id=None, route_desc=None, route_url=None, route_color=None, route_text_color=None):
        res['route_id'] = route_id
        res['short_name'] = route_short_name
        res['long_name'] = route_long_name
        if route_type == '0':
            res['type'] = Route.RouteType.tram
        elif route_type == '1':
            res['type'] = Route.RouteType.subway
        elif route_type == '2':
            res['type'] = Route.RouteType.rail
        elif route_type == '3':
            res['type'] = Route.RouteType.bus
        elif route_type == '4':
            res['type'] = Route.RouteType.ferry
        elif route_type == '5':
            res['type'] = Route.RouteType.cablecar
        elif route_type == '6':
            res['type'] = Route.RouteType.gondola
        elif route_type == '7':
            res['type'] = Route.RouteType.funicular
        else:
            raise Exception("Invalid route_type: %s" % route_type)
        res['agency_id'] = agency_id
        res['desc'] = route_desc
        res['url'] = route_url
        res['color'] = route_color
        res['text_color'] = route_text_color

class Trip(Loader):
    filename = "trips.txt"
    TripDirection = mk_enum('TripDirection', 'undefined', 'inbound', 'outbound')
    reprs = ('route_id', 'service_id', 'trip', 'headsign')
    @classmethod
    def create(cls, res, route_id, service_id, trip_id, trip_headsign=None, trip_short_name=None, direction_id=None, block_id=None, shape_id=None):
        res['route_id'] = route_id
        res['service_id'] = service_id
        res['trip'] = trip_id
        res['headsign'] = trip_headsign
        res['short_name'] = trip_short_name
        if direction_id == '0':
            res['direction'] = Trip.TripDirection.outbound
        elif direction_id == '1':
            res['direction'] = Trip.TripDirection.inbound
        else:
            direction = Trip.TripDirection.undefined
        res['block_id'] = block_id
        res['shape_id'] = shape_id

class StopTime(Loader):
    filename = "stop_times.txt"
    VisitType = mk_enum('VisitType', 'scheduled', 'unavailable', 'phoneahead', 'coordinate')
    reprs = ('trip_id', 'arrival_time', 'stop_sequence')
    @classmethod
    def hms(cls, s):
        "convert a noon minus twelve hour time to seconds"
        h, m, s = map(int, s.split(':'))
        return h * 3600 + m * 60 + s

    @classmethod
    def create(cls, res, trip_id, arrival_time, departure_time, stop_id, stop_sequence, stop_headsign=None, pickup_type=None, drop_off_type=None, shape_dist_travelled=None):
        res['trip_id'] = trip_id
        res['arrival_time'] = StopTime.hms(arrival_time)
        res['departure_time'] = StopTime.hms(departure_time)
        res['stop_id'] = stop_id
        res['stop_sequence'] = int(stop_sequence)
        assert(res['stop_sequence'] >= 0)
        res['headsign'] = stop_headsign
        def visit_type(s):
            if s == '1':
                return StopTime.VisitType.unavailable
            elif s == '2':
                return StopTime.VisitType.phoneahead
            elif s == '3':
                return StopTime.VisitType.coordinate
            else:
                return StopTime.VisitType.scheduled
        res['pickup_type'] = visit_type(pickup_type)
        res['drop_off_type'] = visit_type(drop_off_type)
        res['shape_dist_travelled'] = shape_dist_travelled

class Calendar(Loader):
    filename = 'calendar.txt'
    Day = mk_enum('Day', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday')
    reprs = ('service_id', 'active')
    @classmethod
    def day_from_datetime(cls, dt):
        wd = dt.weekday()
        val = Calendar.Day.values[wd] # 0 - Monday
        print(dt, wd, val)
        return val

    @classmethod
    def create(cls, res, service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date):
        res['service_id'] = service_id
        res['active'] = set()
        def active(e, s):
            if s == '1':
                res['active'].add(e)
                return True
            elif s != '0':
                raise Exception("invalid service activity: '%s'" % s)
        active(Calendar.Day.monday, monday)
        active(Calendar.Day.tuesday, tuesday)
        active(Calendar.Day.wednesday, wednesday)
        active(Calendar.Day.thursday, thursday)
        active(Calendar.Day.friday, friday)
        active(Calendar.Day.saturday, saturday)
        active(Calendar.Day.sunday, sunday)
        res['start_date'] = parse_gtfs_date(start_date)
        res['end_date'] = parse_gtfs_date(end_date)

class CalendarDates(Loader):
    filename = 'calendar_dates.txt'
    ExceptionType = mk_enum('ExceptionType', 'add', 'remove')
    reprs = ('service_id', 'date')
    @classmethod
    def create(cls, res, service_id, date, exception_type):
        res['service_id'] = service_id
        res['date'] = parse_gtfs_date(date)
        if exception_type == '1':
            res['exception'] = CalendarDates.ExceptionType.add
        elif exception_type == '2':
            res['exception'] = CalendarDates.ExceptionType.remove
        else:
            raise Exception("invalid CalendarDate exception_type '%s'" % exception_type)

class GTFS:
    def __init__(self, data_dir):
        for cls in sorted(LoaderMeta.loaders, key=lambda cls: cls.__name__):
            nm = cls.__name__
            print("loading %s" % (nm), file=sys.stderr)
            objs = list(cls.load(data_dir))
            setattr(self, nm, objs)
            print("... (%d loaded)" % (len(objs)), file=sys.stderr)

if __name__ == '__main__':
    print("loading..", file=sys.stderr)
    gtfs = GTFS(sys.argv[1])
