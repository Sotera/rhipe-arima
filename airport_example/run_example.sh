# Copyright 2016 Sotera Defense Solutions Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
