
use gtfs;
import gtfs::gtfs_load;
import gtfs::{feedaccess};

fn main(args: [str])
{
    let feed = gtfs_load(args[1]);
    io::println(feed.describe());
}

