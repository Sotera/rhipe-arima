#!/bin/bash

set -e
# fail-fast - if any part of the script fails, it all exits

#############################
# Runs the scripts necessary to prepare the data for the Rhipe ARIMA script to operate over
# Offloads the data preparation portion to hive for easier extensibility and allows rhipe_arima.R to do all the hard work
#############################

python airport_example/hive/airport_departures_transform.py --source-table-name="airport_deps" --destination-table-name="airport_deps_timeseries"

##############################
# The following script takes in a file format of ID\tDateTime (to an appropriate level of granularity) and an argument of
# hours, minutes, secs to allow us to run our Arima over.
##############################
Rscript rhipe_arima.R /user/hive/warehouse/airport_deps_timeseries /analytics/arima/airport days

##############################
# Copy temporary aggregated tsv output from script and place it in hdfs
##############################
hadoop fs -rm /analytics/arima/airport_standardized_deviations
hadoop fs -copyFromLocal output.txt /analytics/arima/airport_standardized_deviations

