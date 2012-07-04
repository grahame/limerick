#!/usr/bin/env python3

import sys, datetime
import gtfs

def simulate_events(transit, dt, agency):
    service_ids = transit.active_service_ids(dt)
    route_index = dict(((t.route_id, t) for t in transit.Route))
    trips = list(transit.running_trips(route_index, service_ids))
    stop_times = []
    for trip in trips:
        stop_times += transit.stop_times_for_trip_id(trip.trip_id)
    stop_times.sort(key=lambda x: x.arrival_time)
    print("active trips: %d stop_times: %d" % (len(trips), len(stop_times)))
    trip_lookup = dict(((t.trip_id, t) for t in trips))
    stop_lookup = dict(((s.stop_id, s) for s in transit.Stop))
    for stop_time in stop_times:
        stop = stop_lookup[stop_time.stop_id]
        trip = trip_lookup[stop_time.trip_id]
        route = route_index[trip.route_id]
        print("%s: %s %s %s : %s" % (gtfs.StopTime.timestr(stop_time.arrival_time), route.type, route.short_name, trip.headsign, stop.name))
    return stop_times

if __name__ == '__main__':
    dt = datetime.datetime.strptime(sys.argv[2], "%Y-%m-%d").date()
    agency = sys.argv[3]
    transit = gtfs.GTFS(sys.argv[1])
    view = gtfs.GTFSView(transit, agency)
    simulate_events(view, dt, agency)

