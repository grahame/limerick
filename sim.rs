
use std;
import std::map;
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
    stops : map::hashmap<str, stop>
};

iface feedaccess {
    fn nagencies() -> uint;
    fn nstops() -> uint;
}

type agency = {
    name: str,
    url: str,
    timezone: str,
    lang: option<str>,
    phone: option<str>,
    fare_url: option<str>
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

fn gtfs_load(dir: str) -> feed
{
    let file_iter = fn@(fname: str, reqd: [str], f: fn(m: map::hashmap<str,str>)) -> result::t<uint, str> {
        let path = std::fs::connect(dir, fname);
        let res = std::io::file_reader(path);
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
        std::io::println("");
        m.keys() { |k| 
            let v = m.get(k);
            std::io::println(#fmt("'%s' -> %s", k, v));
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
                std::io::println(#fmt("unparsable floating point field `%s' -> `%s'.", k, v));
                (false, 0.)
            }
        }
    }

    let agencies : map::hashmap<str, agency> = map::new_str_hash();
    file_iter("agency.txt", ["agency_name", "agency_url", "agency_timezone"]) { |m| 
        let id = getdefault(m, "agency_id", "_");
        agencies.insert(id, {
            name: m.get("agency_name"),
            url: m.get("agency_url"),
            timezone: m.get("agency_timezone"),
            lang: getoption(m, "agency_lang"),
            phone: getoption(m, "agency_phone"),
            fare_url: getoption(m, "agency_fare_url")
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

    ret { agencies : agencies, stops: stops };
}

impl of feedaccess for feed {
    fn nagencies() -> uint { self.agencies.size() }
    fn nstops() -> uint { self.stops.size() }
}

fn main(args: [str])
{
    let feed = gtfs_load(args[1]);
    std::io::println(#fmt("<gtfs> loaded %u agencies, %u stops.", feed.nagencies(), feed.nstops()));
}

