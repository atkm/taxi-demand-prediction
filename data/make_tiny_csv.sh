#!/bin/bash

set -u
set -e

begin_year=2014
end_year=2014
begin_month=1
end_month=12
# the number of records 
tiny_size=10000

for year in $(seq $begin_year $end_year);
do
    for month in $(seq -f "%02g" $begin_month $end_month);
    do
        small_fname=yellow_tripdata_$year-${month}_small.csv
        tiny_fname=yellow_tripdata_$year-${month}_tiny.csv
        echo -n "Creating $tiny_fname... "
        head -n 1 $small_fname > $tiny_fname
        tail -n +2 $small_fname | shuf -n $tiny_size >> $tiny_fname
        echo "Done."
    done
done
