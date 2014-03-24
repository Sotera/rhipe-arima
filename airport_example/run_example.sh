#!/usr/bin/env bash

set -e 

if [ ! -f airport_deps_2008.tsv ]; 
then
    tar -zxvf airport_deps_2008.tar.gz
fi

if ! hadoop fs -test -d /tmp/airport_deps_sample; 
then
    hadoop fs -mkdir /tmp/airport_deps_sample
fi

if ! hadoop fs -test -e /tmp/airport_deps_sample/airport_deps_2008.tsv; 
then
    hadoop fs -put airport_deps_2008.tsv /tmp/airport_deps_sample/airport_deps_2008.tsv
fi

Rscript ../rhipe_arima.R /tmp/airport_deps_sample /analytics/arima/airport days 4
