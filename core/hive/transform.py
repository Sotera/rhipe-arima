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
  SELECT $SOURCE_ID_COLUMN as id,
         simple_date_format($SOURCE_TIMESTAMP_COLUMN, "$SOURCE_TIMESTAMP_FORMAT", "$DESTINATION_TIMESTAMP_FORMAT") as dtg
  FROM $SOURCE_TABLE_NAME
""")

def execute_hql(hql):
    subprocess.call(["hive", "-e", hql])

def main():
  parser = OptionParser()
  parser.add_option("--source-table-name", dest="source_table_name", help="The table in hive to use as a source for this transform")
  parser.add_option("--destination-table-name", dest="destination_table_name", help="The new table to create; this table will be populated with the results of this transform (Note: any existing table with this name will be dropped before creating a new one!)")
  parser.add_option("--source-id-column", dest="source_id_column", help="The name of the 'id' column in the source table - the column that should be used as the identifier")
  parser.add_option("--source-timestamp-column", dest="source_timestamp_column", help="The name of the timestamp column in the source table")
  parser.add_option("--source-timestamp-format", dest="source_timestamp_format", help="A Java SimpleDateFormat format string")
  parser.add_option("--destination-timestamp-format", dest="destination_timestamp_format", help="A Java SimpleDateFormat format string - this field should be the way in which our times are truncated for ARIMA to work over (days, hours, minutes, seconds)")

  (options, args) = parser.parse_args()
  if not options.source_table_name or \
     not options.destination_table_name or \
     not options.source_id_column or \
     not options.source_timestamp_column or \
     not options.source_timestamp_format or \
     not options.destination_timestamp_format:

    parser.error("All arguments are required")

  hive_common_udf = os.environ['HIVE_COMMON_UDF']
  if hive_common_udf:  # we have the environment variable specified already
    hql = hql_template.substitute(HIVE_COMMON_UDF=hive_common_udf,
                            SOURCE_TABLE_NAME=options.source_table_name,
                            DESTINATION_TABLE_NAME=options.destination_table_name,
                            SOURCE_ID_COLUMN=options.source_id_column,
                            SOURCE_TIMESTAMP_COLUMN=options.source_timestamp_column,
                            SOURCE_TIMESTAMP_FORMAT=options.source_timestamp_format,
                            DESTINATION_TIMESTAMP_FORMAT=options.destination_timestamp_format)
    execute_hql(hql)
  else:
    print "HIVE_COMMON_UDF is not specified as an environment variable; ensure this is package is installed and your environment has sourced /etc/environment before proceeding"


if __name__ == "__main__":
    main()