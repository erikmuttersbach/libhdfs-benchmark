#!/bin/bash
for file in `find /usr/local/hadoop/ -name *.jar`; do export CLASSPATH=$CLASSPATH:$file; done
export LD_LIBRARY_PATH="$JAVA_HOME/jre/lib/amd64/server/"
export LIBHDFS_OPTS="-Djava.library.path=/usr/local/hadoop/lib/native"
