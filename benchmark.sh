#!/bin/bash

buffer_size=4096
file="time_$buffer_size.csv"

for ((i=1; i <= 3; i++))
do 
	n=`expr $i \* 10`
	echo -n "$n," >> $file
	`which time` -f "%e" -a -o $file ./clion1 /tmp/100M $n $buffer_size
done

# Opens gnuplot to visualize the results
#gnuplot -p <<- EOF
#	set title "$buffer_size"
#	set datafile separator ","
#	plot '$file' using 1:0
#EOF
