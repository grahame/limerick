#!/bin/bash -x

c()
{
    rustc -L ../rust-csv/ $*
}

c ../rust-csv/csv.rc &&
c sim.rs

