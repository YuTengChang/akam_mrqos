# old cluster:
#export PATH=$PATH:/a/shark/spark/:/a/third-party/hive/bin/:/a/bin/:/home/testgrp/bin/:/a/third-party/hadoop/bin-mapreduce1/:/home/testgrp/pig/pig-0.11.0-cdh4.6.0/bin
export PATH=$PATH:/a/third-party/hadoop/bin/:/a/third-party/hive/bin/:/a/bin/:/a/shark/shark/shark-0.8.0/bin/:/a/shark/spark/:/usr/local/akamai/java64/jdk1.7.0/bin/
#alias python2.7='/a/bin/python2.7'
source /a/etc/akamai.conf
export hdfs_d='/ghostcache/hadoop/data'
export HADOOP_CONF_DIR='/a/third-party/hadoop/conf'
export HADOOP_USER_NAME=akamai

# Spark related
alias pyspark_cluster='MASTER=spark://23.3.136.52:7077  pyspark'

# PIG related (from Kevin)
PIG_BASE='/home/testgrp/pig'
PIG_HOME="$PIG_BASE/pig-0.11.0-cdh4.6.0"
export PIG_CONF_DIR="$PIG_HOME/conf"
export PIG_CLASSPATH="$PIG_HOME/lib"
export HADOOP_MAPRED_HOME="$HADOOP_HOME"
alias pig11="pig -Dpig.additional.jars=$PIG_CLASSPATH/piggybank.jar:$PIG_CLASSPATH/avro-1.7.6.jar:$PIG_CLASSPATH/avro-mapred-1.7.6-hadoop2.jar:$PIG_CLASSPATH/json-simple-1.1.jar:$PIG_CLASSPATH/snappy-java-1.0.4.1.jar:$PIG_CLASSPATH/jackson-core-asl-1.9.13.jar:$PIG_CLASSPATH/jackson-mapper-asl-1.9.13.jar -Dudf.import.list=org.apache.pig.piggybank.storage.avro"

# dircolors and related aliases
alias ls='ls --color=auto'
alias dir='dir --color=auto'
alias vdir='vdir --color=auto'
alias grep='grep --color=auto'
alias fgrep='fgrep --color=auto'
alias egrep='egrep --color=auto'
alias hls='hadoop fs -ls'
alias bln='beeline -u jdbc:hive2:// -n "" -p "" --silent=false --showHeader=false --outputformat=tsv2'
  