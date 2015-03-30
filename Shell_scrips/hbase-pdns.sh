#!/bin/bash

#Path to jar produce by Maven.
path_to_jar=~/HBase/Query_tool/DNSTools/target/QueryTool-1.0-SNAPSHOT-job.jar
user_home=/home/amir
classpath=$path_to_jar:/usr/local/hadoop/*:/usr/local/hadoop/lib/*:/usr/local/hive/lib/*:$user_home/.m2/repository/args4j/args4j/2.0.24/args4j-2.0.24.jar   

# Run the query tool with the input arguments.
java -cp "$classpath" dnsTools.QueryTool $@
