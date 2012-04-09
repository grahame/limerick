
use gtfs;
use std;
import gtfs::gtfs_load;
import gtfs::{feedaccess};

fn main(args: [str])
{
    let agency_id = args[1];
    let dstr = args[2];
    let data_dir = args[3];

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
        month: tm.tm_mon as uint,
        year: tm.tm_year as uint + 1900u
    };

    io::println(#fmt("%? %?", day, date));

    let feed = gtfs_load(data_dir);
    let service_ids = feed.active_service_ids(day, date);
    let trip_ids = feed.trip_ids_for_service_ids(service_ids);
    let trips = vec::filter(feed.lookup_trips(trip_ids)) { |trip|
        let route = feed.lookup_routes([trip.route_id])[0];
        route.agency_id == agency_id
    };
    let events = feed.events(trip_ids);

    io::println(#fmt("%u active services, %u active trips.", vec::len(service_ids), vec::len(trips)));
}

