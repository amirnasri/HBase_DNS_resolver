#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "usage: $0 <local_input_file> <hdfs_output_file> <hbase_table_name>"
    exit 1
fi

input_file=$1
output_file=$2
table_name=$3

# Check if input file exists
ls $input_file &> /dev/null
if [ $? -ne 0 ]; then
    echo "Input file does not exist."
    exit 1
fi

# Check if input file already exists
# If so, delete the old file and copy the new one
hadoop fs -ls /tmp/$input_file &> /dev/null
if [ $? -eq 0 ]; then
    hadoop fs -rmr /tmp/$input_file
fi
hadoop fs -copyFromLocal $input_file /tmp/$input_file &> /dev/null

# Check if output file already exists
# If so, delete the file
hadoop fs -ls $output_file &> /dev/null
if [ $? -eq 0 ]; then
    hadoop fs -rmr $output_file
fi

echo -e "disable '$table_name'\n" "drop '$table_name'\n" "create '$table_name', 'cf'\n" | hbase shell

# Path to jar produce by Maven.
path_to_jar=../Maven/DNSTools/target/QueryTool-1.0-SNAPSHOT-job.jar

# Run map-reduce job to bulk-load data from input_file to HBase
hadoop jar "$path_to_jar" dnsTools.BulkLoader /tmp/$input_file $output_file $table_name

