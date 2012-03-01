#!/bin/bash -x

c()
{
    rustc -g -O -L ../rust-csv/ $*
}

c ../rust-csv/csv.rc &&
c sim.rs

