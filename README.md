DNS resolver using Hadoop and HBase
----------------------------------------

* `DNSTools`  folder  contains   a   maven   project   with   two   main   classes:   the
`BulkLoader`   class   and   the   `QueryTool`   class. `BulkLoader` class performs the data bulkload to HBase. This class sets up the
MR configuration and also provide the mapper code. The reduce task is taken care of
by the   HfileOutputFormat.configureIncrementalLoad() function. After the data is
loaded   to   an   Hfile,   it   is   loaded   to   an   HBase   table   using   LoadIncrementalHFiles
class.
`QueryTool`
class is responsible for parsing and performing the queries.

* The shell script  `load_data.sh`  can be used to copy a local file to hdfs, and
then   run   an   MR   job   to   perform   the   bulk-load   operation. Example usage:

		./load_data.sh  dns_log_file  output_file table_name 

* The shell script `hbase-pdns.sh` runs the `QueryTool` jar file to perform
the query give by the input arguments.

* Installation   files:  The   main   installation   files   for   Hadoop,   HBase,   and   Hive are in the `Installation_files` folder. 
