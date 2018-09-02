#!/bin/bash

set -u
set -e

begin_year=2009
end_year=2013
begin_month=1
end_month=12
size=small
for year in $(seq $begin_year $end_year);
do
    for month in $(seq -f "%02g" $begin_month $end_month);
    do
        gcloud compute scp shell-1:yellow_tripdata_${year}-${month}_${size}.csv .
    done
done
