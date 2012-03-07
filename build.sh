#!/bin/bash -x

c()
{
    rustc -g -L ../rust-csv/ $*
}

c ../rust-csv/csv.rc &&
c sim.rs

