# HDFS Benchmark

## Compiling

In order to be able to run libhdfs benchmarks, specify the include directory and the library path when compiling: 

`$ cd libhdfs-benchmark`
`$ mkdir build`
`$ cd build`
`$ cmake -DLIBHDFS_INCLUDE_DIR=/usr/local/include/hdfs -DLIBHDFS_LIBRARY=/usr/local/lib/libhdfs3.so ..`


## Running the benchmarks

### Clearing the File System Cache
If you run the benchmark as root, e.g. with sudo, the file system cache will be cleared automatically. If you prefer to run the benchmark without root rights, run the following commands to drop the file system cache:  

`$ sudo sync`  
`$ sudo echo 3 | sudo tee /proc/sys/vm/drop_caches`

### Measuring disk read and write speed with dd

`$ dd if=/dev/zero of=tempfile bs=1M count=1024 conv=fdatasync,notrunc`
`$ echo 3 | sudo tee /proc/sys/vm/drop_caches`
`$ dd if=tempfile of=/dev/null bs=1M count=1024`
`$ dd if=tempfile of=/dev/null bs=1M count=1024`

