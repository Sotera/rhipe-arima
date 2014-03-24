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

A base example is provided, working on the Flight Data that can be found [here](http://stat-computing.org/dataexpo/2009/the-data.html) - [http://stat-computing.org/dataexpo/2009/the-data.html](http://stat-computing.org/dataexpo/2009/the-data.html).  

> Rscript rhipe_arima.R /tmp/airport_deps_sample /analytics/arima/airport days 4

The first argument after rhipe_arima.R is the input file.  
The second is the output directory.  
The third is how to aggregate the data.  Possible values for this third argument are __days,hours,minutes,secs__
The fourth is the threshold, in standard deviations, a residual must be above to be emitted   


Other Information
-----------------

Please make sure you can read and write from the __/tmp__ directory as well as any other directory you specify. 
