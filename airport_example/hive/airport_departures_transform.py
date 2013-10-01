from optparse import OptionParser
from string import Template
import os
import subprocess

hql_template = Template("""
  SOURCE $HIVE_COMMON_UDF/hql/base.hql;
  SOURCE $HIVE_COMMON_UDF/hql/SimpleDateFormat.hql;

  DROP TABLE $DESTINATION_TABLE_NAME;
  CREATE TABLE $DESTINATION_TABLE_NAME (id string, dtg string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

  INSERT OVERWRITE TABLE $DESTINATION_TABLE_NAME
  SELECT
    Origin as id,
    simple_date_format(concat(Year, "-", Month, "-", DayofMonth, " ", substring(lpad(DepTime, 4, "0000"), 1, 2), ":", substring(lpad(DepTime, 4, "0000"), 3, 2)),
                       "yyyy-MM-dd HH:mm",
                       "yyyy-MM-dd HH:00:00") as dtg
    FROM $SOURCE_TABLE_NAME where DepTime != "NA" AND
         simple_date_format(concat(Year, "-", Month, "-", DayofMonth, " ", substring(lpad(DepTime, 4, "0000"), 1, 2), ":", substring(lpad(DepTime, 4, "0000"), 3, 2)),
                       "yyyy-MM-dd HH:mm",
                       "yyyy-MM-dd HH:00:00") IS NOT NULL AND
         (Origin = "SLC" OR
          Origin = "ATL" OR
          Origin = "MSP" OR
          Origin = "ORD" OR
          Origin = "DFW" OR
          Origin = "IAH" OR
          Origin = "PIT" OR
          Origin = "DEN" OR
          Origin = "ONT" OR
          Origin = "LAX" OR
          Origin = "BWI" OR
          Origin = "PDX" OR
          Origin = "SEA" OR
          Origin = "MCI" OR
          Origin = "SFO" OR
          Origin = "PHX" OR
          Origin = "LAS" OR
          Origin = "MSY" OR
          Origin = "TPA" OR
          Origin = "FLL" OR
          Origin = "CLE" OR
          Origin = "SAN" OR
          Origin = "ABQ" OR
          Origin = "SJC" OR
          Origin = "CVG" OR
          Origin = "SAT" OR
          Origin = "HOU" OR
          Origin = "CLT" OR
          Origin = "JFK" OR
          Origin = "BNA" OR
          Origin = "SMF" OR
          Origin = "IAD" OR
          Origin = "LGA" OR
          Origin = "STL" OR
          Origin = "DTW" OR
          Origin = "MCO" OR
          Origin = "MIA" OR
          Origin = "BOS" OR
          Origin = "PH" OR
          Origin = "EWR" OR
          Origin = "OAK");
""")

def execute_hql(hql):
    subprocess.call(["hive", "-e", hql])

def main():
  parser = OptionParser()
  parser.add_option("--source-table-name", dest="source_table_name", help="The table in hive to use as a source for this transform")
  parser.add_option("--destination-table-name", dest="destination_table_name", help="The new table to create; this table will be populated with the results of this transform (Note: any existing table with this name will be dropped before creating a new one!)")

  (options, args) = parser.parse_args()
  if not options.source_table_name or \
     not options.destination_table_name:
    parser.error("All arguments are required")

  hive_common_udf = os.environ['HIVE_COMMON_UDF']
  if hive_common_udf:  # we have the environment variable specified already
    hql = hql_template.substitute(HIVE_COMMON_UDF=hive_common_udf,
                            SOURCE_TABLE_NAME=options.source_table_name,
                            DESTINATION_TABLE_NAME=options.destination_table_name)
    execute_hql(hql)
  else:
    print "HIVE_COMMON_UDF is not specified as an environment variable; ensure this is package is installed and your environment has sourced /etc/environment before proceeding"


if __name__ == "__main__":
    main()
