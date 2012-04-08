#!/bin/bash -x

extra=$*

c()
{
    rustc -g $extra -L ../rust-csv/ -L . $*
}

c ../rust-csv/csv.rc &&
c gtfs.rc &&
c bbox.rs

