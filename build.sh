#!/bin/bash -x

extra=$*

c()
{
    rustc -g $extra -L ../rust-csv/ $*
}

c ../rust-csv/csv.rc &&
c sim.rs

