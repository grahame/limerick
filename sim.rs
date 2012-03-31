
use std;
import std::map;
import map::hashmap;
import std::sort;
use csv;
import csv::rowreader;
import csv::{rowiter};

/* we want to build these higher-level concepts;
   [ Agency ]
     -> Routes
     -> Trips [ for a Service ]
         -> Stop Times
     -> Calendar [ for a Service]
     -> CalendarDates [ for a Service ]
*/

type row = map::hashmap<str, str>;

type agencies = map::hashmap<str, @agency>;
type stops = map::hashmap<str, @stop>;
type routes = map::hashmap<str, @route>;
type trips = map::hashmap<str, @trip>;
type stop_times = map::hashmap<str, [ @stop_time ]>;
type calendars = map::hashmap<str, @calendar>;
type calendar_dates = map::hashmap<str, [@calendar_date]>;

type feed = {
    agencies: agencies,
    stops : stops,
    routes: routes,
    trips: trips,
    stop_times: stop_times,
    calendars: calendars, 
    calendar_dates: calendar_dates,
};

type agency = {
    id: str,
    name: str,
    url: str,
    timezone: str,
    lang: option<str>,
    phone: option<str>,
    fare_url: option<str>,
};

enum route_type {
    tram(),
    subway(),
    rail(),
    bus(),
    ferry(),
    cable_car(),
    gondola(),
    funicular()
}

type route = {
    id: str,
    agency_id: str,
    short_name: str,
    long_name: str,
    desc: option<str>,
    route_type: route_type,
    url: option<str>,
    color: option<str>,
    text_color: option<str>,
};

type point = {
    lat : float,
    lon : float
};

enum location_type {
    location_stop(),
    location_station()
}

type stop = {
    id: str,
    code: option<str>,
    name: str,
    pt: point,
    desc: option<str>,
    zone_id: option<str>,
    url: option<str>,
    location_type: option<location_type>,
    parent_station: option<str>,
    timezone: option<str>
};

enum direction {
    oneway(),
    theotherway()
}

type trip = {
    id: str,
    route_id: str,
    service_id: str,
    headsign: option<str>,
    short_name: option<str>,
    direction: option<direction>,
    block_id: option<str>,
    shape_id: option<str>
};

enum time {
    unspecified(),
    relnoon(uint)
}

enum marshal {
    scheduled(),
    nopickup(),
    phoneahead(),
    coordinatewithdriver()
}

type stop_time = {
    trip_id: str,
    arrival_time: time,
    departure_time: time,
    stop_id: str,
    sequence: uint,
    headsign: option<str>,
    pickup_type: option<marshal>,
    drop_off_type: option<marshal>,
    shape_dist_travelled: option<float>
};

type date = {
    day: uint,
    month: uint,
    year: uint
};

type calendar = {
    service_id: str,
    monday: bool,
    tuesday: bool,
    wednesday: bool,
    thursday: bool,
    friday: bool, 
    saturday: bool,
    sunday: bool,
    start_date: date,
    end_date: date
};

enum exception {
    service_added(),
    service_removed()
}

type calendar_date = {
    service_id: str,
    date: date,
    exception_type: exception
};

fn gtfs_load(dir: str) -> feed
{
    fn file_iter(path: str, reqf: [(uint, str)], optf: [(uint, str)], f: fn(row: [str], req: [uint], opt: [option<uint>])) {
        io::println("loading file: " + path);
        let res = io::file_reader(path);
        if result::failure(res) {
            let error : str = result::get_err(res);
            fail(#fmt("cannot open %s: %s", path, error));
        }
        let reader = csv::new_reader(result::get(res), ',', '"');
        let mut row = [];
        if !reader.readrow(row) {
            fail(#fmt("%s: no column row", path));
        }

        let header = vec::map(row) { |t| str::trim(t) };

        let mut req_lookup = [];
        let mut i = 0u;
        vec::iter(reqf) { |field|
            let (enumval, fieldname) = field;
            assert(enumval == i);
            let pos = vec::position_elem(header, fieldname);
            alt pos {
                some(pos) { req_lookup += [pos] }
                none { fail("required field not found in file") }
            }
            i += 1u;
        };
        let mut opt_lookup = [];
        let mut i = 0u;
        vec::iter(optf) { |field|
            let (enumval, fieldname) = field;
            assert(enumval == i);
            opt_lookup += [vec::position_elem(header, fieldname)];
            i += 1u;
        };
        while reader.readrow(row) {
            f(row, req_lookup, opt_lookup);
        }
    };

    fn no_overwrite<T: copy, U: copy>(m: map::hashmap<T, U>, k: T, v: U) {
        if ! m.insert(k, v) {
            log(error, ("no_overwrite: duplicate key", k));
            fail;
        }
    }


    fn getdefault(row: [str], offset: option<uint>, default: str) -> str {
        alt offset {
            some(n) { row[n] }
            none { default }
        }
    }

    fn getoption(row: [str], offset: option<uint>) -> option<str> {
        alt offset {
            some(n) { some(row[n]) }
            none { none }
        }
    }

    fn floatfail(s: str) -> float {
        alt float::from_str(s) {
            some(n) { n }
            none { fail("cannot convert str to floating point") }
        }
    }

    fn getdate(s: str) -> date {
        fn usub(s: str, offset: uint, len: uint) -> uint {
            alt uint::from_str(str::substr(s, offset, len)) {
                some(f) { f }
                none { fail("invalid year code") }
            }
        }
        let year = usub(s, 0u, 4u);
        let month = usub(s, 4u, 2u);
        let day = usub(s, 6u, 2u);
        assert(month <= 12u);
        assert(day <= 31u);
        {
            day: day,
            month: month,
            year: year,
        }
    }

    fn load_agencies(fname: str, agencies: agencies) {
        enum req { name, url, timezone }
        let reqf = [
            (name as uint, "agency_name"),
            (url as uint, "agency_url"),
            (timezone as uint, "agency_timezone")
                ];
        enum opt { id, lang, phone, fare_url }
        let optf = [
            (id as uint, "agency_id" ),
            (lang as uint, "agency_lang"),
            (phone as uint, "agency_phone"),
            (fare_url as uint, "agency_fare_url")
                ];
        file_iter(fname, reqf, optf) { |row, req, opt|
            let row_id = getdefault(row, opt[id as uint], "_");
            no_overwrite(agencies, row_id, @{
                id: row_id, 
                name: row[req[name as uint]], 
                url: row[req[url as uint]],
                timezone: row[req[timezone as uint]],
                lang: getoption(row, opt[lang as uint]),
                phone: getoption(row, opt[phone as uint]),
                fare_url: getoption(row, opt[fare_url as uint])
            });
        };
    };

    fn load_stops(fname: str, stops: stops) {
        fn get_location_type(loc: option<str>) -> option<location_type> {
            alt loc{
                some(s) {
                    if s == "" || s == "0" {
                        some(location_stop)
                    } else if s == "1" {
                        some(location_station)
                    } else {
                        fail("impossible location")
                    }
                }
                none { none }
            }
        }
        enum req { id, name, lat, lon };
        let reqf = [
            (id as uint, "stop_id"),
            (name as uint, "stop_name"),
            (lat as uint, "stop_lat"),
            (lon as uint, "stop_lon")
                ];
        enum opt { code, desc, zone_id, url, location_type, parent_station, timezone }
        let optf = [
            (code as uint, "stop_code"),
            (desc as uint, "stop_desc"),
            (zone_id as uint, "zone_id"),
            (url as uint, "stop_url"),
            (location_type as uint, "location_type"),
            (parent_station as uint, "parent_station"),
            (timezone as uint, "stop_timezone")
                ];
        file_iter(fname, reqf, optf) { |row, req, opt|
            let stop_id = row[req[id as uint]];
            no_overwrite(stops, stop_id, @{
                id: stop_id, 
                code: getoption(row, opt[code as uint]),
                name : row[req[name as uint]],
                pt : {
                    lat : floatfail(row[req[lat as uint]]), 
                    lon : floatfail(row[req[lon as uint]]), 
                },
                desc: getoption(row, opt[desc as uint]),
                zone_id: getoption(row, opt[zone_id as uint]),
                url: getoption(row, opt[url as uint]),
                location_type: get_location_type(getoption(row, opt[location_type as uint])),
                parent_station: getoption(row, opt[parent_station as uint]),
                timezone: getoption(row, opt[timezone as uint])
            });
        };
    };

    fn load_routes(fname: str, routes: routes) {
        fn get_route_type(rt: str) -> route_type {
            alt rt {
                "0" { tram }
                "1" { subway }
                "2" { rail }
                "3" { bus }
                "4" { ferry }
                "5" { cable_car }
                "6" { gondola }
                "7" { funicular }
                _ { fail("invalid route type") }
            }
        }
        enum req { route_id, short_name, long_name, route_type };
        let reqf = [
            (route_id as uint, "route_id"),
            (short_name as uint, "route_short_name"),
            (long_name as uint, "route_long_name"),
            (route_type as uint, "route_type")
                ];
        enum opt { agency_id, desc, url, color, text_color };
        let optf = [
            (agency_id as uint, "agency_id"),
            (desc as uint, "route_desc"),
            (url as uint, "route_url"),
            (color as uint, "route_color"),
            (text_color as uint, "route_text_color")
                ];
        file_iter(fname, reqf, optf) { |row, req, opt|
            let id = row[req[route_id as uint]];
            no_overwrite(routes, id, @{
                id: id,
                agency_id: getdefault(row, opt[agency_id as uint], "_"),
                short_name: row[req[short_name as uint]],
                long_name: row[req[long_name as uint]],
                desc: getoption(row, opt[desc as uint]),
                route_type: get_route_type(row[req[route_type as uint]]),
                url: getoption(row, opt[url as uint]),
                color: getoption(row, opt[color as uint]),
                text_color: getoption(row, opt[text_color as uint])
            });
        };
    }

    fn load_trips(fname: str, trips: trips) {
        fn getdirection(d: option<str>) -> option<direction> {
            alt d {
                some(s) {
                    alt(s) {
                        "0" { some(oneway) }
                        "1" { some(theotherway) }
                        _   { fail("invalid direction_id") }
                    }
                }
                none { none }
            }
        }
        enum req { route_id, service_id, trip_id };
        let reqf = [
            (route_id as uint, "route_id"),
            (service_id as uint, "service_id"),
            (trip_id as uint, "trip_id")
                ];
        enum opt { headsign, short_name, direction_id, block_id, shape_id }
        let optf = [
            (headsign as uint, "trip_headsign"),
            (short_name as uint, "trip_short_name"),
            (direction_id as uint, "direction_id"),
            (block_id as uint, "block_id"),
            (shape_id as uint, "shape_id")
                ];
        file_iter(fname, reqf, optf) { |row, req, opt|
            let id = row[req[trip_id as uint]];
            no_overwrite(trips, id, @{
                id: id, 
                route_id: row[req[route_id as uint]],
                service_id: row[req[service_id as uint]],
                headsign: getoption(row, opt[headsign as uint]),
                short_name: getoption(row, opt[short_name as uint]),
                direction: getdirection(getoption(row, opt[direction_id as uint])),
                block_id: getoption(row, opt[block_id as uint]),
                shape_id: getoption(row, opt[shape_id as uint]),
            });
        };
    }
    fn load_stop_times(fname: str, stop_times: stop_times) {
        fn gettime(s: str) -> time {
            if s == "" {
                unspecified
            } else {
                let tc : [str] = str::split_char(s, ':');
                if vec::len(tc) != 3u {
                    fail("incorrect time component length");
                }
                let lens = vec::map(tc, {|t| str::len(t)});
                assert(lens[0] == 1u || lens[0] == 2u);
                assert(lens[1] == 2u);
                assert(lens[2] == 2u);
                let mut secs = 0u;
                alt uint::from_str(tc[0]) {
                    some(v) { secs += v * 3600u; }
                    _ { fail("invalid hour"); }
                }
                fn minsec(s: str) -> uint {
                    alt uint::from_str(s) {
                        some(v) {
                            if v > 59u {
                                fail("invalid minute");
                            } else {
                                v
                            }
                        }
                        _ { fail("invalid minute"); }
                    }
                }
                secs += minsec(tc[1]) * 60u;
                secs += minsec(tc[2]);
                relnoon(secs)
            }
        }
        fn getmarshal(s: option<str>) -> option<marshal> {
            alt s {
                some(v) {
                    alt v {
                        "0" { some(scheduled) }
                        "1" { some(nopickup) }
                        "2" { some(phoneahead) }
                        "3" { some(coordinatewithdriver) }
                        _ { fail("unknown marshal type") }
                    }
                }
                none { none }
            }
        }
        enum req { trip_id, arrival_time, departure_time, stop_id, stop_sequence };
        let reqf = [
            (trip_id as uint, "trip_id"),
            (arrival_time as uint, "arrival_time"),
            (departure_time as uint, "departure_time"),
            (stop_id as uint, "stop_id"),
            (stop_sequence as uint, "stop_sequence")
                ];
        enum opt { headsign, pickup_type, drop_off_type, travelled };
        let optf = [
            (headsign as uint, "stop_headsign"),
            (pickup_type as uint, "pickup_type"),
            (drop_off_type as uint, "drop_off_type"),
            (travelled as uint, "shape_dist_travelled")
                ];

        file_iter(fname, reqf, optf) { |row,req,opt|
            let seq = alt uint::from_str(row[req[stop_sequence as uint]]) {
                some(v) { v }
                _ { fail("invalid stop_sequence") }
            };
            let id = row[req[trip_id as uint]];
            let mut trip_list = if stop_times.contains_key(id) {
                stop_times.get(id)
            } else {
                let mut n = [];
                vec::reserve(n, 16u);
                n
            };
            let time =  @ {
                trip_id: id, 
                arrival_time: gettime(row[req[arrival_time as uint]]),
                departure_time: gettime(row[req[departure_time as uint]]),
                stop_id: row[req[stop_id as uint]],
                sequence: seq,
                headsign: getoption(row, opt[headsign as uint]),
                pickup_type: getmarshal(getoption(row, opt[pickup_type as uint])),
                drop_off_type: getmarshal(getoption(row, opt[drop_off_type as uint])),
                shape_dist_travelled: alt getoption(row, opt[travelled as uint]) {
                    some(s) {
                        alt float::from_str(s) {
                            some(f) { some(f) }
                            none { fail("invalid shape_dist_travelled") }
                        }
                    }
                    none { none }
                }
            };
            trip_list += [ time ];
            stop_times.insert(id, trip_list);
        };
    }
    fn load_calendars(fname: str, calendars: calendars) {
        fn getbool(s: str) -> bool {
            alt(s) {
                "0" { false }
                "1" { true }
                _   { fail("invalid boolean value") }
            }
        }
        enum req { id, mon, tue, wed, thu, fri, sat, sun, start, end };
        let reqf = [
            (id as uint, "service_id"),
            (mon as uint, "monday"),
            (tue as uint, "tuesday"),
            (wed as uint, "wednesday"),
            (thu as uint, "thursday"),
            (fri as uint, "friday"),
            (sat as uint, "saturday"),
            (sun as uint, "sunday"),
            (start as uint, "start_date"),
            (end as uint, "end_date")
                ];

        file_iter(fname, reqf, []) { |row,req,opt|
            let service_id = row[req[id as uint]];
            no_overwrite(calendars, service_id, @{
                service_id: service_id,
                monday: getbool(row[req[mon as uint]]),
                tuesday: getbool(row[req[tue as uint]]),
                wednesday: getbool(row[req[wed as uint]]),
                thursday: getbool(row[req[thu as uint]]),
                friday: getbool(row[req[fri as uint]]),
                saturday: getbool(row[req[sat as uint]]),
                sunday: getbool(row[req[sun as uint]]),
                start_date: getdate(row[req[start as uint]]),
                end_date: getdate(row[req[end as uint]])
            });
        };
    }
    fn load_calendar_dates(fname: str, calendar_dates: calendar_dates) {
        fn get_exception(s: str) -> exception {
            alt s {
                "1" { service_added }
                "2" { service_removed }
                _ { fail }
            }
        }
        enum req { id, date, exception_type }
        let req = [
            (id as uint, "service_id"),
            (date as uint, "date"),
            (exception_type as uint, "exception_type")
                ];
        file_iter(fname, req, []) { |row,req,opt|
            let service_id = row[req[id as uint]];
            let mut service_dates = if (calendar_dates.contains_key(service_id)) {
                calendar_dates.get(service_id)
            } else { 
                []
            };
            let calendar_date = @{
                service_id: service_id,
                date: getdate(row[req[date as uint]]),
                exception_type: get_exception(row[req[exception_type as uint]])
            };
            service_dates += [ calendar_date ];
            calendar_dates.insert(service_id, service_dates);
        };
    }

    fn hash_list_sort<T:copy>(m: map::hashmap<str,[T]>, le: fn(T,T) -> bool) -> map::hashmap<str,[T]> {
        let new_hash = map::str_hash();
        m.items() { |k,v|
            new_hash.insert(k, sort::merge_sort(le, v));
        };
        ret new_hash;
    }


    let agencies : agencies = map::str_hash();
    let trips : trips = map::str_hash();
    let stops : stops = map::str_hash();
    let routes : routes = map::str_hash();
    let mut stop_times : stop_times = map::str_hash();
    let calendars : calendars = map::str_hash();
    let calendar_dates : calendar_dates = map::str_hash();

    load_agencies(path::connect(dir, "agency.txt"), agencies);
    load_stops(path::connect(dir, "stops.txt"), stops);
    load_routes(path::connect(dir, "routes.txt"), routes);
    load_trips(path::connect(dir, "trips.txt"), trips);
    load_stop_times(path::connect(dir, "stop_times.txt"), stop_times);
    /* we can't assume stop times are sorted in input file */
    stop_times = hash_list_sort(stop_times) { |v1, v2|
        v1.sequence <= v2.sequence
    };
    load_calendars(path::connect(dir, "calendar.txt"), calendars);
    load_calendar_dates(path::connect(dir, "calendar_dates.txt"), calendar_dates);

    ret {
        agencies : agencies,
        stops: stops,
        routes: routes, 
        trips: trips,
        stop_times: stop_times,
        calendars: calendars,
        calendar_dates: calendar_dates
    };
}

iface feedaccess {
    fn describe() -> str;
}

impl of feedaccess for feed {
    fn describe() -> str {
        #fmt("%u agencies, %u stops, %u routes, %u trips, %u stop_times, %u calendars, %u calendar_dates",
            self.agencies.size(), self.stops.size(), self.routes.size(), self.trips.size(), self.stop_times.size(),
            self.calendars.size(), self.calendar_dates.size())
    }
}

fn main(args: [str])
{
    let feed = gtfs_load(args[1]);
    io::println(feed.describe());
}

