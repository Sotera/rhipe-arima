Rhipe ARIMA
============================

This implementation of the ARIMA (AutoRegressive Integrated Moving Average) algorithm is based on R, Hadoop, and the RHIPE (Hree-pay) framework.

Rhipe webpage: <a href="http://www.datadr.org/getpack.html">Rhipe</a>


Rhipe Requirements
------------------
- R
- Hadoop (CDH 4.3.0 is our target)
- Google Protocol Buffers (2.4.1)


Analytic Requirements
---------------------

*  R packages forecast and chron
*  [Hive Common Udf](https://github.com/Sotera/hive-common-udf) - Only if you want an external hive query to put your time data in YYYY-MM-DD HH:mm:SS format.



Input
-----


The Rhipe ARIMA analytic assumes the data will be in the following format:

> IDENTIFIER\tTIMESTAMP(YYYY-MM-DD HH:mm:SS)


Output
------
It will also output the data in the following format:


> IDENTIFIER\tTIMESTAMP(YYYY-MM-DD HH:mm:SS)\tSTANDARDIZED_RESIDUALS(a floating point number)



Example
-------
</br>

A base example is provided, working on the Flight Data that can be found [here](http://stat-computing.org/dataexpo/2009/the-data.html) - [http://stat-computing.org/dataexpo/2009/the-data.html](http://stat-computing.org/dataexpo/2009/the-data.html) 

The data, however much you download, must be loaded into a hive table called airport_deps . 

Inspect the airport_example.sh file.  The first python line 
> python airport_example/hive/airport_departures_transform.py --source-table-name="airport_deps" --destination-table-name="airport_deps_timeseries"

is needed to transform the data into the required input format found above.  Originally, this data has seperated Day, Month, Year, Time  into different fields, additionally, there are no seconds.  Because of this we use the <HIVE_COMMON_UDF> SimpleDateFormat function to format the data.

__If your data is already in YYYY-MM-DD HH:mm:SS format__ but has more columns than the input format describes, then you can easily use the __core/hive/transform.py__ file.  This function will execute a hive query from the --source-table-name into the --destination-table-name . 


The next line in __airport_traffic.sh__ actually runs the analytic.

> Rscript rhipe_arima.R /user/hive/warehouse/airport_deps_timeseries /analytics/arima/airport days

The first argument after rhipe_arima.R is the input file.  
The second is the output directory.  
The third is how to aggregate the data.  Possible values for this third argument are __days,hours,minutes,secs__


Other Information
-----------------

Please make sure you can read and write from the __/tmp__ directory as well as any other directory you specify.  If executing the example, this means you must have permissions to __create and read Hive tables__ and be able to __write to the /analytics/arima/airport directory__. 