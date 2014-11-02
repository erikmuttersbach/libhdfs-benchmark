#!/bin/bash


echo "i,time" > time.csv
for ((i=1; i <= 10; i++))
do 
	n=`expr $i \* 10`
	echo -n "$n," >> time.csv
	`which time` -f "%e" -a -o time.csv ./clion1 /tmp/100M $n
done

# Opens gnuplot to visualize the results
gnuplot -p <<- EOF
	set datafile separator ","
	plot 'time.csv' using 0:1
EOF
