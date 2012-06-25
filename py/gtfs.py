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
            ndefs = len(args.defaults or [])
            reqd = args.args[1:-ndefs] # skip self argument
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
            args = [None] * len(indices)
            for row in reader:
                yield cls(*[row[t] for t in indices], **(dict([(t, row[optdict[t]]) for t in optdict])))

class Agency(Loader):
    filename = "agency.txt"
    reprs = ('agency_id', 'name')
    def __init__(self, agency_name, agency_url, agency_timezone, agency_id=None, agency_lang=None, agency_phone=None, agency_fare_url=None):
        self.agency_id = agency_id or "default"
        self.name = agency_name
        self.url = agency_url
        self.timezone = agency_timezone
        self.lang = agency_lang
        self.phone = agency_phone
        self.fare_url = agency_fare_url

def mk_enum(type_name, *args):
    return type(type_name, (), dict((t, i) for (i, t) in enumerate(args)))

def parse_gtfs_date(s):
    return datetime.datetime.strptime(s, "%Y%m%d").date()

class Stop(Loader):
    filename = "stops.txt"
    reprs = ('stop_id', 'name')
    LocationType = mk_enum('LocationType', 'stop', 'station')
    WheelchairBoarding = mk_enum('WheelchairBoarding', 'unknown', 'possibly', 'none')
    def __init__(self, stop_id, stop_name, stop_lat, stop_lon, stop_code=None, stop_desc=None, zone_id=None, stop_url=None, location_type=None, parent_station=None, stop_timezone=None, wheelchair_boarding=None):
        self.stop_id = stop_id
        self.name = stop_name
        self.latlng = map(float, (stop_lat, stop_lon))
        self.code = stop_code
        self.desc = stop_desc
        self.zone_id = zone_id
        self.url = stop_url
        if location_type == '1':
            self.location_type = Stop.LocationType.station
        else:
            self.location_type = Stop.LocationType.stop
        self.parent_station = parent_station
        self.timezone = stop_timezone
        if wheelchair_boarding == '1':
            self.wheelchair_boarding = Stop.WheelchairBoarding.possibly
        elif wheelchair_boarding == '2':
            self.wheelchair_boarding = Stop.WheelchairBoarding.none
        else:
            self.wheelchair_boarding = Stop.WheelchairBoarding.unknown

class Route(Loader):
    filename = "routes.txt"
    RouteType = mk_enum('RouteType', 'tram', 'subway', 'rail', 'bus', 'ferry', 'cablecar', 'gondola', 'funicular')
    def __init__(self, route_id, route_short_name, route_long_name, route_type, agency_id=None, route_desc=None, route_url=None, route_color=None, route_text_color=None):
        self.route_id = route_id
        self.short_name = route_short_name
        self.long_name = route_long_name
        if route_type == '0':
            self.type = Route.RouteType.tram
        elif route_type == '1':
            self.type = Route.RouteType.subway
        elif route_type == '2':
            self.type = Route.RouteType.rail
        elif route_type == '3':
            self.type = Route.RouteType.bus
        elif route_type == '4':
            self.type = Route.RouteType.ferry
        elif route_type == '5':
            self.type = Route.RouteType.cablecar
        elif route_type == '6':
            self.type = Route.RouteType.gondola
        elif route_type == '7':
            self.type = Route.RouteType.funicular
        else:
            raise Exception("Invalid route_type: %s" % route_type)
        self.agency_id = agency_id
        self.desc = route_desc
        self.url = route_url
        self.color = route_color
        self.text_color = route_text_color

class Trip(Loader):
    filename = "trips.txt"
    TripDirection = mk_enum('TripDirection', 'undefined', 'inbound', 'outbound')
    def __init__(self, route_id, service_id, trip_id, trip_headsign=None, trip_short_name=None, direction_id=None, block_id=None, shape_id=None):
        self.route_id = route_id
        self.service_id = service_id
        self.trip = trip_id
        self.headsign = trip_headsign
        self.short_name = trip_short_name
        if direction_id == '0':
            self.direction = Trip.TripDirection.outbound
        elif direction_id == '1':
            self.direction = Trip.TripDirection.inbound
        else:
            self.direction = Trip.TripDirection.undefined
        self.block_id = block_id
        self.shape_id = shape_id

class StopTime(Loader):
    filename = "stop_times.txt"
    VisitType = mk_enum('VisitType', 'scheduled', 'unavailable', 'phoneahead', 'coordinate')
    class HMS:
        def __init__(self, s):
            self.h, self.m, self.s = map(int, s.split(':'))
    def __init__(self, trip_id, arrival_time, departure_time, stop_id, stop_sequence, stop_headsign=None, pickup_type=None, drop_off_type=None, shape_dist_travelled=None):
        self.trip_id = trip_id
        self.arrival_time = StopTime.HMS(arrival_time)
        self.departure_time = StopTime.HMS(departure_time)
        self.stop_id = stop_id
        self.stop_sequence  = int(stop_sequence)
        assert(self.stop_sequence >= 0)
        self.headsign = stop_headsign
        def visit_type(s):
            if s == '1':
                return StopTime.VisitType.unavailable
            elif s == '2':
                return StopTime.VisitType.phoneahead
            elif s == '3':
                return StopTime.VisitType.coordinate
            else:
                return StopTime.VisitType.scheduled
        self.pickup_type = visit_type(pickup_type)
        self.drop_off_type = visit_type(drop_off_type)
        self.shape_dist_travelled = shape_dist_travelled

class Calendar:
    filename = 'calendar.txt'
    def __init__(self, service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date):
        self.service_id = service_id
        def active(s):
            if s == '1':
                return True
            elif s == '0':
                return False
            else:
                raise Exception("invalid service activity: '%s'" % s)
        self.monday = active(monday)
        self.tuesday = active(tuesday)
        self.wednesday = active(wednesday)
        self.thursday = active(thursday)
        self.friday = active(friday)
        self.saturday = active(saturday)
        self.sunday = active(sunday)
        self.start_date = parse_gtfs_date(start_date)
        self.end_date = parse_gtfs_date(end_date)

class CalendarDates:
    filename = 'calendar_dates.txt'
    ExceptionType = mk_enum('ExceptionType', 'add', 'remove')
    def __init__(self, service_id, date, exception_type):
        self.service_id = service_id
        self.date = parse_gtfs_date(date)
        if exception_type == '1':
            self.exception = ExceptionType.add
        elif exception_type == '2':
            self.exception = ExceptionType.remove
        else:
            raise Exception("invalid CalendarDate exception_type '%s'" % exception_type)

class GTFS:
    def __init__(self, data_dir):
        for cls in LoaderMeta.loaders:
            nm = cls.__name__
            objs = list(cls.load(data_dir))
            setattr(self, nm, objs)

if __name__ == '__main__':
    gtfs = GTFS(sys.argv[1])
    print(gtfs.Agency)


