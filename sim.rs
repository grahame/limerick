
use std;
import std::map;
import map::hashmap;
use csv;
import csv::rowreader;
import csv::{rowaccess,rowiter};


/* we want to build these higher-level concepts;

   [ Agency ]
     -> Routes
     -> Trips [ for a Service ]
         -> Stop Times
     -> Calendar [ for a Service]
     -> CalendarDates [ for a Service ]

*/

type row = map::hashmap<str, str>;

type feed = {
    agencies: map::hashmap<str, agency>, 
    stops : map::hashmap<str, stop>,
    routes: map::hashmap<str, route>,
    trips: map::hashmap<str, trip>,
    stop_times: map::hashmap<str, stop_time>,
    calendars: map::hashmap<str, calendar>,
    calendar_dates: map::hashmap<str, calendar_date>,
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
    let file_iter = fn@(fname: str, reqd: [str], f: fn(m: map::hashmap<str,str>)) -> result::t<uint, str> {
        let path = path::connect(dir, fname);
        let res = io::file_reader(path);
        if result::failure(res) {
            ret result::err(result::get_err(res));
        }
        let r = csv::new_reader(result::get(res), ',', '"');
        let nrows = 0u;
        let cols_ok = true;
        let cols_check = fn@(cols: [str]) -> bool {
            let ok = true;
            let i = 0u;
            while i < vec::len(reqd) {
                ok = ok && vec::contains(cols, reqd[i]);
                i += 1u;
            }
            ret ok;
        };
        csv::hashmap_iter_full(r, {|s| str::trim(s)}, {|cols| cols_ok = cols_check(cols); cols_ok}) { |m|
            f(m);
            nrows += 1u;
        };
        if !cols_ok {
            ret result::err("required columns not found");
        }
        ret result::ok(nrows);
    };

    fn dump_row(m: map::hashmap<str, str>) {
        io::println("");
        m.keys() { |k| 
            let v = m.get(k);
            io::println(#fmt("'%s' -> %s", k, v));
        }
    }

    fn getdefault<T, U : copy>(m: map::hashmap<T, U>, k: T, def: U) -> U {
        if m.contains_key(k) {
            m.get(k)
        } else {
            def
        }
    }

    fn getoption<T,U>(m: map::hashmap<T, U>, k: T) -> option<U> {
        if m.contains_key(k) {
            some(m.get(k))
        } else {
            none
        }
    }

    fn getfloat(m: map::hashmap<str, str>, k: str) -> (bool, float) {
        let v = m.get(k);
        alt float::from_str(v) {
            some(n) {
                (true, n)
            }
            none {
                io::println(#fmt("unparsable floating point field `%s' -> `%s'.", k, v));
                (false, 0.)
            }
        }
    }

    let agencies : map::hashmap<str, agency> = map::new_str_hash();
    file_iter("agency.txt", ["agency_name", "agency_url", "agency_timezone"]) { |m| 
        let id = getdefault(m, "agency_id", "_");
        agencies.insert(id, {
            id: getdefault(m, "agency_id", "_"),
            name: m.get("agency_name"),
            url: m.get("agency_url"),
            timezone: m.get("agency_timezone"),
            lang: getoption(m, "agency_lang"),
            phone: getoption(m, "agency_phone"),
            fare_url: getoption(m, "agency_fare_url"),
        });
    };

    let stops : map::hashmap<str, stop> = map::new_str_hash();
    fn get_location_type(loc: option<str>) -> option<location_type> {
        alt loc {
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
    file_iter("stops.txt", ["stop_id", "stop_name", "stop_lat", "stop_lon"]) { |m|
        let (ok, lat) = getfloat(m, "stop_lat");
        if !ok {
            ret;
        }
        let (ok, lon) = getfloat(m, "stop_lon");
        if !ok {
            ret;
        }
        stops.insert(m.get("stop_id"), { 
            id: m.get("stop_id"), 
            code: getoption(m, "stop_code"),
            name : m.get("stop_name"),
            pt : {
                lat : lat,
                lon : lon
            }, 
            desc: getoption(m, "stop_desc"),
            zone_id: getoption(m, "zone_id"),
            url: getoption(m, "stop_url"),
            location_type: get_location_type(getoption(m, "location_type")),
            parent_station: getoption(m, "parent_station"),
            timezone: getoption(m, "stop_timezone")
        });
    };
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
    let routes : map::hashmap<str, route> = map::new_str_hash();
    file_iter("routes.txt", ["route_id", "route_short_name", "route_long_name", "route_type"]) { |m|
        routes.insert(m.get("route_id"), { 
            id: m.get("route_id"), 
            agency_id: getdefault(m, "agency_id", "_"),
            short_name: m.get("route_short_name"),
            long_name: m.get("route_long_name"),
            desc: getoption(m, "route_desc"),
            route_type: get_route_type(m.get("route_type")),
            url: getoption(m, "route_url"),
            color: getoption(m, "route_color"),
            text_color: getoption(m, "route_text_color")
        });
    };
    fn getdirection(d: option<str>) -> option<direction> {
        alt d {
            some(s) {
                alt(s) {
                    "0" { some(oneway) }
                    "1" { some(theotherway) }
                    _   { fail("invalid direction_id") }
                }
            }
            none { 
                none
            }
        }
    }
    let trips : map::hashmap<str, trip> = map::new_str_hash();
    file_iter("trips.txt", ["route_id", "service_id", "trip_id"]) { |m|
        trips.insert(m.get("trip_id"), {
            id: m.get("trip_id"),
            route_id: m.get("route_id"),
            service_id: m.get("service_id"),
            headsign: getoption(m, "trip_headsign"),
            short_name: getoption(m, "trip_short_name"),
            direction: getdirection(getoption(m, "direction_id")),
            block_id: getoption(m, "block_id"),
            shape_id: getoption(m, "shape_id")
        });
    };
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
            let secs = 0u;
            alt uint::from_str(tc[0]) {
                some(v) {
                    secs += v * 3600u;
                }
                _ {
                    fail("invalid hour");
                }
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
                    _ {
                        fail("invalid minute");
                    }
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
            none {
                none
            }
        }
    }
    let stop_times : map::hashmap<str, stop_time> = map::new_str_hash();
    file_iter("stop_times.txt", ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"]) { |m|
        stop_times.insert(m.get("trip_id"), {
            trip_id: m.get("trip_id"),
            arrival_time: gettime(m.get("arrival_time")),
            departure_time: gettime(m.get("departure_time")),
            stop_id: m.get("stop_id"),
            sequence: alt uint::from_str(m.get("stop_sequence")) {
                some(v) { v }
                _ {
                    fail("invalid stop_sequence")
                }
            },
            headsign: getoption(m, "stop_headsign"),
            pickup_type: getmarshal(getoption(m, "pickup_type")),
            drop_off_type: getmarshal(getoption(m, "drop_off_type")),
            shape_dist_travelled: alt getoption(m, "shape_dist_travelled") {
                some(s) {
                    alt float::from_str(s) {
                        some(f) { some(f) }
                        none { fail("invalid shape_dist_travelled") }
                    }
                }
                none { none }
            }
        });
    };
    let calendars : map::hashmap<str, calendar> = map::new_str_hash();
    fn getbool(s: str) -> bool {
        alt(s) {
            "0" { false }
            "1" { true }
            _   { fail("invalid boolean value") }
        }
    }
    fn getdate(s: str) -> date {
        fn usub(s: str, offset: uint, len: uint) -> uint {
            alt uint::from_str(str::substr(s, 0u, 4u)) {
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
    file_iter("calendar.txt", ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]) { |m| 
        calendars.insert(m.get("service_id"), {
            service_id: m.get("service_id"),
            monday: getbool(m.get("monday")),
            tuesday: getbool(m.get("tuesday")),
            wednesday: getbool(m.get("wednesday")),
            thursday: getbool(m.get("thursday")),
            friday: getbool(m.get("friday")),
            saturday: getbool(m.get("saturday")),
            sunday: getbool(m.get("sunday")),
            start_date: getdate(m.get("start_date")),
            end_date: getdate(m.get("end_date"))
        });
    };
    let calendar_dates : map::hashmap<str, calendar_date> = map::new_str_hash();
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
    fn nagencies() -> uint;
    fn nstops() -> uint;
    fn nroutes() -> uint;
    fn ntrips() -> uint;
}

impl of feedaccess for feed {
    fn nagencies() -> uint { self.agencies.size() }
    fn nstops() -> uint { self.stops.size() }
    fn nroutes() -> uint { self.routes.size() }
    fn ntrips() -> uint { self.trips.size() }
}

fn main(args: [str])
{
    let feed = gtfs_load(args[1]);
    io::println(#fmt("<< loaded %u agencies, %u stops, %u routes, %u trips >>",
                feed.nagencies(), feed.nstops(), feed.nroutes(), feed.ntrips()));
}

