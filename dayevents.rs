
use gtfs;
use std;
import std::sort;
import gtfs::gtfs_load;
import gtfs::{feedaccess};

enum event {
    startevents(uint,uint),
    endevents,
    starttrip(uint, ~gtfs::trip),
    endtrip(uint, ~gtfs::trip),
    stoparrival(uint, ~gtfs::trip, ~gtfs::stop_time),
}

fn simulate_events(out: comm::chan<event>, agency_id: str, dstr: str, data_dir: str) {
    let tm = alt std::time::strptime(dstr, "%a %Y-%m-%d") {
        result::ok(d) { d }
        result::err(s) { fail(s) }
    };
    let day = alt tm.tm_wday as int {
        0 { gtfs::sunday }
        1 { gtfs::monday }
        2 { gtfs::tuesday }
        3 { gtfs::wednesday }
        4 { gtfs::thursday }
        5 { gtfs::friday }
        6 { gtfs::saturday }
        _ { fail }
    };
    let date =  {
        day: tm.tm_mday as uint,
        month: tm.tm_mon as uint + 1u,
        year: tm.tm_year as uint + 1900u
    };

    io::println(#fmt("%? %?", day, date));

    let feed = gtfs_load(data_dir);

    let mut trip_stops = { ||
        let service_ids = feed.active_service_ids(day, date);
        let trip_ids = feed.trip_ids_for_service_ids(service_ids);
        let trips = vec::filter(feed.lookup_trips(trip_ids)) { |trip|
            let route = feed.lookup_routes([trip.route_id])[0];
            route.agency_id == agency_id
        };
        comm::send(out, startevents(vec::len(service_ids), vec::len(trips)));
        let mut ts = [mut];
        for vec::each(trips) { |trip|
            let st = feed.lookup_stop_times([trip.id])[0];
            let first_arrival = st[0].arrival_time;
            ts += [ (trip, first_arrival, st) ];
        }
        sort::quick_sort3({|v1,v2| 
            let (_, a, _) = v1;
            let (_, b, _) = v2;
            a < b
        }, {|v1,v2|
            let (_, a, _) = v1;
            let (_, b, _) = v2;
            a == b
        }, ts);
        ts
    }();
    if vec::len(trip_stops) == 0u {
        ret;
    }
    let mut (_, now, _) = trip_stops[0];
    let mut trip_index = 0u;
    type trip_run = {
        trip: @gtfs::trip,
        stop_times: [ mut @gtfs::stop_time ],
        mut offset: uint
    };

    let mut running : [ @trip_run ] = [];
    loop {
        let mut next_time = uint::max_value;
        /* find commencing trips */
        while trip_index < vec::len(trip_stops) {
            let (trip, first_arrival, stop_times) = trip_stops[trip_index];
            assert(first_arrival >= now);
            if first_arrival != now {
                next_time = uint::min(first_arrival, next_time);
                break;
            }
            running += [ @{ trip: trip, stop_times: stop_times, offset: 0u } ];
            comm::send(out, starttrip(now, ~*trip));
            trip_index += 1u;
        }
        /* find stop arrivals & ending trips */
        let mut still_running = [];
        for vec::each(running) { |r|
            while r.offset < vec::len(r.stop_times) {
                let st = r.stop_times[r.offset];
                assert(st.arrival_time >= now);
                if st.arrival_time != now {
                    next_time = uint::min(st.arrival_time, next_time);
                    break;
                }
                comm::send(out, stoparrival(now, ~*r.trip, ~*st));
                r.offset += 1u;
            }
            if r.offset == vec::len(r.stop_times) {
                comm::send(out, endtrip(now, ~*r.trip));
            } else {
                still_running += [ r ];
            }
        }
        running = still_running;
        if vec::len(running) == 0u && trip_index == vec::len(trip_stops) {
            break;
        }
        /* tick */
        now = next_time;
    }
    comm::send(out, endevents);
}

fn main(args: [str])
{
    let port = comm::port::<event>();
    let chan = comm::chan::<event>(port);
    task::spawn { ||
        let agency_id = args[1];
        let dstr = args[2];
        let data_dir = args[3];
        simulate_events(chan, agency_id, dstr, data_dir);
    }
    loop {
        let result = comm::recv(port);
        alt result {
            startevents(ns, nt) {
                io::println(#fmt("%u active services, %u active trips.", ns, nt));
            }
            endevents {
                io::println("end");
                break;
            }
            starttrip(t, trip) { 
                log(error, ("starttrip", t, trip))
            }
            endtrip(t, trip) {
                log(error, ("endtrip", t, trip))
            }
            stoparrival(t, trip, stop) {
                log(error, ("stoparrival", t, trip, stop))
            }
        }
    }
}

