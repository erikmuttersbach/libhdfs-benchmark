# HDFS Benchmark

## Running the benchmark

### Clearing the File System Cache
If you run the benchmark as root, e.g. with sudo, the file system cache will be cleared automatically. If you prefer to run the benchmark without root rights, run the following commands to drop the file system cache:  

`$ sudo sync`  
`$ sudo echo 3 | sudo tee /proc/sys/vm/drop_caches`

### Options

The following 

