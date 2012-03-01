
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
    timezone: str
};

type point = {
    lat : float,
    lon : float
};

type stop = {
    name: str,
    pt: point,
    row: row
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

    fn getfloat<T>(m: map::hashmap<T, str>, k: T) -> (bool, float) {
        let v = m.get(k);
        alt float::from_str(v) {
            some(n) {
                (true, n)
            }
            none {
                (false, 0.)
            }
        }
    }

    let load_agencies = fn@() -> map::hashmap<str, agency> {
        let agencies : map::hashmap<str, agency> = map::new_str_hash();
        file_iter("agency.txt", ["agency_name", "agency_url", "agency_timezone"]) { |m| 
            let id = getdefault(m, "agency_id", "_");
            agencies.insert(id, {
                name: m.get("agency_name"),
                url: m.get("agency_url"),
                timezone: m.get("agency_timezone")
            });
        };
        ret agencies;
    };

    let load_stops = fn@() -> map::hashmap<str, stop> {
        let stops : map::hashmap<str, stop> = map::new_str_hash();
        file_iter("stops.txt", ["stop_id", "stop_name", "stop_lat", "stop_lon"]) { |m|
            let (ok, lat) = getfloat(m, "stop_lat");
            if !ok {
                std::io::println(#fmt("unparsable stop_lat `%s', skipping.", m.get("stop_lat")));
                ret;
            }
            let (ok, lon) = getfloat(m, "stop_lon");
            if !ok {
                std::io::println(#fmt("unparsable stop_lon `%s', skipping.", m.get("stop_lon")));
                ret;
            }
            stops.insert(m.get("stop_id"), { 
                name : m.get("stop_name"),
                pt : {
                    lat : lat,
                    lon : lon
                }, 
                row : m
            });
        };
        ret stops;
    };

    let agencies = load_agencies();
    let stops = load_stops(); 
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

