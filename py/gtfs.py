#!/usr/bin/env python3

import inspect, sys, os, csv, datetime, collections, itertools

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
    @classmethod
    def set_not_none(cls, d, k, v):
        if v is not None:
            d[k] = v

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

class Shape(Loader):
    filename = "shapes.txt"
    @classmethod
    def create(cls, res, shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_travelled=None):
        res['shape_id'] = shape_id
        res['latlng'] = (float(shape_pt_lat), float(shape_pt_lon))
        res['sequence'] = int(shape_pt_sequence)
        assert(res['sequence'] >= 0)
        cls.set_not_none(res, 'dist_travelled', shape_dist_travelled)

class Stop(Loader):
    filename = "stops.txt"
    reprs = ('stop_id', 'name')
    LocationType = mk_enum('LocationType', 'stop', 'station')
    WheelchairBoarding = mk_enum('WheelchairBoarding', 'unknown', 'possibly', 'none')
    @classmethod
    def create(cls, res, stop_id, stop_name, stop_lat, stop_lon, stop_code=None, stop_desc=None, zone_id=None, stop_url=None, location_type=None, parent_station=None, stop_timezone=None, wheelchair_boarding=None):
        res['stop_id'] = stop_id
        res['name'] = stop_name
        res['latlng'] = list(map(float, (stop_lat, stop_lon)))
        cls.set_not_none(res, 'code', stop_code)
        cls.set_not_none(res, 'desc', stop_desc)
        cls.set_not_none(res, 'zone_id', zone_id)
        cls.set_not_none(res, 'url', stop_url)
        if location_type == '1':
            res['location_type']= Stop.LocationType.station
        else:
            res['location_type']= Stop.LocationType.stop
        cls.set_not_none(res, 'parent_station', parent_station)
        cls.set_not_none(res, 'timezone', stop_timezone)
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
        res['agency_id'] = agency_id or "default"
        cls.set_not_none(res, 'desc', route_desc)
        cls.set_not_none(res, 'url', route_url)
        cls.set_not_none(res, 'color', route_color)
        cls.set_not_none(res, 'text_color', route_text_color)

class Trip(Loader):
    filename = "trips.txt"
    TripDirection = mk_enum('TripDirection', 'undefined', 'inbound', 'outbound')
    reprs = ('route_id', 'service_id', 'trip', 'headsign')
    @classmethod
    def create(cls, res, route_id, service_id, trip_id, trip_headsign=None, trip_short_name=None, direction_id=None, block_id=None, shape_id=None):
        res['route_id'] = route_id
        res['service_id'] = service_id
        res['trip_id'] = trip_id
        cls.set_not_none(res, 'headsign', trip_headsign)
        cls.set_not_none(res, 'short_name', trip_short_name)
        if direction_id == '0':
            res['direction'] = Trip.TripDirection.outbound
        elif direction_id == '1':
            res['direction'] = Trip.TripDirection.inbound
        else:
            direction = Trip.TripDirection.undefined
        cls.set_not_none(res, 'block_id', block_id)
        cls.set_not_none(res, 'shape_id', shape_id)

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
    def timestr(cls, s):
        h = s // 3600
        s %= 3600
        m = s // 60
        s %= 60
        return "%.2d:%.2d:%.2d" % (h, m, s)

    @classmethod
    def create(cls, res, trip_id, arrival_time, departure_time, stop_id, stop_sequence, stop_headsign=None, pickup_type=None, drop_off_type=None, shape_dist_travelled=None):
        res['trip_id'] = trip_id
        res['arrival_time'] = StopTime.hms(arrival_time)
        res['departure_time'] = StopTime.hms(departure_time)
        res['stop_id'] = stop_id
        res['stop_sequence'] = int(stop_sequence)
        assert(res['stop_sequence'] >= 0)
        cls.set_not_none(res, 'headsign', stop_headsign)
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
        cls.set_not_none(res, 'shape_dist_travelled', shape_dist_travelled)

class Calendar(Loader):
    filename = 'calendar.txt'
    Day = mk_enum('Day', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday')
    reprs = ('service_id', 'active')
    @classmethod
    def day_from_datetime(cls, dt):
        wd = dt.weekday()
        val = Calendar.Day.values[wd] # 0 - Monday
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
    def stop_times_for_trip_id(self, trip_id):
        return self.trip_stop_times[trip_id]
    
    @staticmethod
    def _calc_bounds(latlng_fn):
        "returns sw, ne, pass latlngfn which returns a generator with interesting latlngs"
        minlat = min((t[0] for t in latlng_fn()))
        maxlat = max((t[0] for t in latlng_fn()))
        minlng = min((t[1] for t in latlng_fn()))
        maxlng = max((t[1] for t in latlng_fn()))
        return (minlat, minlng), (maxlat, maxlng)

    def bounds(self):
        def latlngs():
            return itertools.chain((t.latlng for t in self.Shape), (t.latlng for t in self.Stop))
        return self._calc_bounds(latlngs)
    
    def active_service_ids(self, dt):
        day = Calendar.day_from_datetime(dt)
        service_ids = set()
        for cal in self.Calendar:
            if day in cal.active:
                service_ids.add(cal.service_id)
        cal_dates = [t for t in self.CalendarDates if t.date == dt]
        for cal_date in cal_dates:
            if cal_date.exception == CalendarDates.ExceptionType.add:
                service_ids.add(cal_date.service_id)
            elif cal_date.exception == CalendarDates.ExceptionType.remove:
                try:
                    service_ids.remove(cal_date.service_id)
                except KeyError:
                    pass
        return service_ids

    def agency_routes(self, agency_id):
        routes = {}
        for route in self.Route:
            if route.agency_id == agency_id:
                routes[route.route_id] = route
        return routes

    def running_trips(self, routes, service_ids):
        for trip in self.Trip:
            if self.route_id in routes and self.service_id in service_ids:
                yield trip

class GTFSDataset(GTFS):
    "gtfs dataset from disk files"
    def __init__(self, data_dir):
        for cls in sorted(LoaderMeta.loaders, key=lambda cls: cls.__name__):
            nm = cls.__name__
            print("loading %s" % (nm), file=sys.stderr)
            objs = list(cls.load(data_dir))
            setattr(self, nm, objs)
            print("... (%d loaded)" % (len(objs)), file=sys.stderr)
        self.trip_stop_times = {}
        for stop_time in self.StopTime:
            trip_id = stop_time.trip_id
            if trip_id not in self.trip_stop_times:
                self.trip_stop_times[trip_id] = []
            self.trip_stop_times[trip_id].append(stop_time)
        for trip_id in self.trip_stop_times:
            self.trip_stop_times[trip_id].sort(key=lambda x: x.stop_sequence)

class GTFSView(GTFS):
    "view of a GTFS dataset, reduced to one agency"
    def __init__(self, parent, agency_id):
        self.Agency = [t for t in parent.Agency if t.agency_id == agency_id]
        assert(len(self.Agency) == 1)
        self.Route = [t for t in parent.Route if t.agency_id == agency_id]
        route_ids = set((t.route_id for t in self.Route))
        self.Trip = [t for t in parent.Trip if t.route_id in route_ids]
        trip_ids = set((t.trip_id for t in self.Trip))
        shape_ids = set(filter(None, (t.shape_id for t in self.Trip)))
        self.Shape = [t for t in parent.Shape if t.shape_id in shape_ids]
        self.StopTime = [t for t in parent.StopTime if t.trip_id in trip_ids]
        stop_ids = set((t.stop_id for t in self.StopTime))
        service_ids = set((t.service_id for t in self.Trip))
        self.Calendar = [t for t in parent.Calendar if t.service_id in service_ids]
        self.CalendarDates = [t for t in parent.CalendarDates if t.service_id in service_ids]
        self.Stop = [t for t in parent.Stop if t.stop_id in stop_ids]

if __name__ == '__main__':
    print("loading..", file=sys.stderr)
    transit = GTFSDataset(sys.argv[1])
    view = GTFSView(transit, "1")
    print("bounds: (lat,lng) ", view.bounds())

