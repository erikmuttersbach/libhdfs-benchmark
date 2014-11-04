#include <iostream>
#include <tbb/task.h>

#include "hdfs.h"

using namespace std;

timespec timespec_diff(timespec start, timespec end) {
    timespec temp;
    if ((end.tv_nsec-start.tv_nsec)<0) {
        temp.tv_sec = end.tv_sec-start.tv_sec-1;
        temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
    } else {
        temp.tv_sec = end.tv_sec-start.tv_sec;
        temp.tv_nsec = end.tv_nsec-start.tv_nsec;
    }
    return temp;
}

/**
* TODO:
*  - get Short circuit reads to work
*  - use readzero reads (mmap)
*/
int main(int argc, char *argv[]) {
    // Setup and parse options
    int n = 1;
    int concurrency = 1;
    const char *path = "/tmp/1M";
    size_t bufferSize = 1024;
    bool useRz = true;
    if(argc == 4) {
        path = argv[1];
        n = atoi(argv[2]);
        bufferSize = atoi(argv[3]);
    }

    printf("Reading %s %i times\n", path, n);

    struct timespec startTime, endTime;
    clock_gettime(CLOCK_MONOTONIC, &startTime);

    // using default,0 as parameters doesn't work. I THINK, short circuit
    // reads should be used then but don't work because native libs can't be loaded??
    hdfsFS fs = hdfsConnect("127.0.0.1", 9000);

    hdfsFile file = hdfsOpenFile(fs, path, O_RDONLY, 4096, 0, 0);
    if(!file) {
        fprintf(stderr, "Failed to open %s for reading!\n", path);
        exit(1);
    }

    tSize totalRead = 0;
    char *buffer = (char*)malloc(sizeof(char)*bufferSize);

    struct hadoopRzOptions *rzOptions;
    struct hadoopRzBuffer *rzBuffer;
    if(useRz) {
        rzOptions = hadoopRzOptionsAlloc();

        if(rzOptions == NULL) {
            fprintf(stderr, "Failed to allocate zero read buffer\n");
            exit(1);
        }
    }

    for(int i=0; i<n; i++) {
        tSize read = 0;
        do {
            if(useRz) {
                rzBuffer = hadoopReadZero(file, rzOptions, 0);
                if(rzBuffer == NULL) {
                    fprintf(stderr, "Failed to perform zero read for path %s (%i)\n", path, errno);
                    exit(1);
                }

                const void *data = hadoopRzBufferGet(rzBuffer);

                read += hadoopRzBufferLength(rzBuffer);
                hadoopRzBufferFree(file, rzBuffer);
            } else {
                read = hdfsRead(fs, file, buffer, bufferSize);
            }

            totalRead += read;
        } while (read > 0);

        if(!useRz) {
            //hdfsSeek(fs, file, 0);
        }
    }

    free(buffer);

    clock_gettime(CLOCK_MONOTONIC, &endTime);
    struct timespec time = timespec_diff(startTime, endTime);

    // TODO Should we use 2^10 or 1024 for KB, MB, GB?
    printf("Read %f GB in %lfs with a buffer size of %i\n", ((float)totalRead)/(1000.0*1000.0*1000.0), time.tv_sec + time.tv_nsec/1000000000.0, (int)bufferSize);

    struct hdfsReadStatistics *stats;
    hdfsFileGetReadStatistics(file, &stats);
    printf("Statistics:\n\tTotal: %lld\n\tLocal: %lld\n\tShort Circuit: %lld\n\tZero Read: %lld\n", stats->totalBytesRead, stats->totalLocalBytesRead, stats->totalShortCircuitBytesRead, stats->totalZeroCopyBytesRead);
    hdfsFileFreeReadStatistics(stats);

    if(rzOptions) {
        hadoopRzOptionsFree(rzOptions);
    }

    hdfsCloseFile(fs, file);
}