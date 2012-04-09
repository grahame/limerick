
use gtfs;
use std;
import gtfs::gtfs_load;
import gtfs::{feedaccess};

fn main(args: [str])
{
    let agency_id = args[1];
    let date = args[2];
    let data_dir = args[3];

    let tm = alt std::time::strptime(date, "%Y-%m-%d") {
        result::ok(d) { d }
        result::err(s) { fail(s) }
    };

    io::println(#fmt("playing back transport events for %d %d %d day %d",
                tm.tm_year as int, tm.tm_mon as int, tm.tm_mday as int, tm.tm_wday as int));


    //let feed = gtfs_load(data_dir);
}

