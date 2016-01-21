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

args<-commandArgs(TRUE)

if (length(args) < 4) {
    # we want to throw an error and exit
    print("This script requires arguments in the form of INPUT_PATH OUTPUT_PATH PERIOD(days,hours,minutes,secs) STANDARD_DEV_THRESHOLD")
    q(save="no",status=1)
}

input_path = args[1]
output_path = args[2]
period = args[3]
stdev_threshold = as.numeric(args[4])
intermediate_output_path = paste("/tmp",output_path, sep="")

library(Rhipe)
rhinit()

column_delimiter = "\t"

####################
# Ugly code - probably could be far more clever using R properly
####################
if (length(args) > 5) {
    if (args[5] == "--delim") {
        column_delimiter = args[6]
    } else if (args[5] == "--cluster-config") {
        source(args[6])
    }
}

if (length(args) == 8) {
    if (args[7] == "--delim") {
        column_delimiter = args[8]
    } else if (args[7] == "--cluster-config") {
        source(args[8])
    }
}

hdfs.setwd("/")

#mapreduce job to format raw text from hive table
clean_map <- expression({
    for(row in map.values){
        split_row <- unlist(strsplit(row,column_delimiter))#splits the input
        rhcollect(split_row[1],c(split_row[1],split_row[2]))
    }
})

clean_reduce <- expression(
    pre       = { s <- NULL },
    reduce    = { s <- c(s,reduce.values)},
    post      = {
        temp <- data.frame(t(sapply(s,c))) #creates a dataframe from the raw text
        #this long line creates a time stamp by extracting the digits using math functions
        temp$dates<-as.POSIXct(temp$X2,"%Y-%m-%d %H:%M:%S", tz="UTC")
        temp$freq<-1
        temp$dates<-trunc(temp$dates,period)
        arima_freq_count<-aggregate(temp$freq,list(as.character(temp$dates)),sum) #aggregate sums up
        names(arima_freq_count)<-c("dt","freq")
        arima_freq_count$id<-reduce.key
        rhcollect(reduce.key,arima_freq_count[,c("id","dt","freq")])
    }
)

arima_map <- expression({
    for(r in map.values){
        #important to suppress all output during map-reduce jobs or rhipe will fail
        suppressMessages(suppressWarnings(library(forecast)))
        suppressMessages(suppressWarnings(library(chron)))
        empty <- seq(as.POSIXct(r$dt[1]), as.POSIXct(r$dt[length(r$dt)]),by=period) #timestamp dataframe
        empty <- data.frame(as.character(empty))
        names(empty) <- "empty"
        arima_seq = merge(empty,r[,c("dt","freq")],by.x="empty",by.y="dt",all.x="TRUE") #merge freq with timestamp vector
        arima_seq$freq[is.na(arima_seq$freq)]<-0 #replaces NA's with 0's
        invisible(mod<-auto.arima(ts(arima_seq$freq))) #fits the data to the arima model, invisible prevents stdout
        resid<-mod$resid/sd(mod$resid) #calculates standardized residuals
        #ind<-which(abs(resid)>4) #finds the indices of the std residuals above a threshold
        ind<-which(abs(resid)>stdev_threshold) #finds the indices of the std residuals above a threshold
        if (length(ind) > 0) {
            out<-cbind(as.character(arima_seq$empty[ind]),as.character(resid[ind]))#collects origin, date, and residual
            #out<-cbind(as.character(arima_seq$empty),as.character(resid))#collects origin, date, and residual
            out_df <- data.frame(out)
            out_df$id <- apply(out_df, 1, function(row) r$id[1])
            names(out_df) <- c("dt","std_res", "id")
            rhcollect(r$id[1], out_df)
        }
    }
})

mapReduceOut <- rhwatch(map=clean_map,reduce=clean_reduce,input=rhfmt(type="text", folders=input_path),parameters=list(period=period),output=intermediate_output_path, readback=FALSE)
mapReduceOut <- rhwatch(map=arima_map,input=intermediate_output_path,output=output_path, readback=FALSE)

input <- rhread(output_path)
map.values <- lapply(input, "[[", 2)

# converts the resuling map.values into a dataframe.
map.values <- lapply(map.values, function(x) {
   data.frame(key=x[3], date=x[1], std_res=x[2], stringsAsFactors=FALSE)
})


# combines the list of data frames into one data frame.
# this is supposed to be the "slow" way.  The fast way is:
#      library(data.table)
#      data <- data.frame(rbindlist(map.values))
# however then using write.table caused everything to crash.

data <- do.call(rbind, map.values)

#deletes the file
unlink("output.txt")

  
#write out the one large dataframe.
write.table(data, file="output.txt", append=FALSE, quote=FALSE, sep="\t", row.names=FALSE, col.names=FALSE)

 




  
  
  
