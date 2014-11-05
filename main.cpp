#include <iostream>
#include <string.h>
#include <tbb/task.h>
#include <hdfs.h>

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

#define EXPECT_NONZERO(r, func) if(r==NULL) { \
                                    fprintf(stderr, "%s failed: %s\n", func, strerror(errno)); \
                                    exit(1); \
                                }

void use_data(void *data, size_t length) {
    volatile int c = 0;
    for(size_t i=0; i<length; i++) {
        c += *((char*)data+i);
    }
}

bool read_zcr(hdfsFS fs, hdfsFile file, hdfsFileInfo *fileInfo, int n, size_t buffer_size) {
    struct hadoopRzOptions *rzOptions;
    struct hadoopRzBuffer *rzBuffer;

    // Initialize zcr
    rzOptions = hadoopRzOptionsAlloc();
    EXPECT_NONZERO(rzOptions, "hadoopRzOptionsAlloc")

    hadoopRzOptionsSetSkipChecksum(rzOptions, true); // TODO Play with this parameter
    hadoopRzOptionsSetByteBufferPool(rzOptions, ELASTIC_BYTE_BUFFER_POOL_CLASS);

    size_t total_read = 0, read = 0;
    do {
        rzBuffer = hadoopReadZero(file, rzOptions, buffer_size);
        if(rzBuffer != NULL) {
            const void *data = hadoopRzBufferGet(rzBuffer);
            read = hadoopRzBufferLength(rzBuffer);
            total_read += read;

            if(read > 0) {
                use_data((void*)data, read);
            }

            hadoopRzBufferFree(file, rzBuffer);
        }
        else if(errno == EOPNOTSUPP) {
            printf("zcr not supported\n");
            return false;
        }
        else {
            EXPECT_NONZERO(rzBuffer, "hadoopReadZero");
        }
    } while(read > 0);

    if(total_read != fileInfo[0].mSize) {
        fprintf(stderr, "Failed to zero-copy read file to full size\n");
        exit(1);
    }

    hadoopRzOptionsFree(rzOptions);

    return true;
}

bool read_standard(hdfsFS fs, hdfsFile file, hdfsFileInfo *fileInfo, int n, size_t buffer_size) {
    char *buffer = (char*)malloc(sizeof(char)*buffer_size);

    tSize total_read = 0, read = 0;
    do {
        read = hdfsRead(fs, file, buffer, buffer_size);

        if(read > 0) {
            use_data(buffer, read);
        }

        total_read += read;
    } while (read > 0);

    if(total_read != fileInfo[0].mSize) {
        fprintf(stderr, "Failed to read file to full size. Read %luB\n", fileInfo[0].mSize);
        exit(1);
    }

    free(buffer);
}

/**
* TODO:
*  - get Short circuit reads to work
*  - use readzero reads (mmap)
*  - implement threading
*/
int main(int argc, char *argv[]) {
    // Setup and parse options
    int n = 1;
    const char *path = "/tmp/1000M";
    size_t buffer_size = 4096;
    bool force_standard_read = false;
    if(argc >= 4) {
        path = argv[1];
        n = atoi(argv[2]);
        buffer_size = atoi(argv[3]);
    }
    if(argc >= 5) {
        force_standard_read = strcmp(argv[4], "std");
    }

    printf("Reading %s with %i threads\n", path, n);

    struct timespec startTime, openedTime, endTime;
    clock_gettime(CLOCK_MONOTONIC, &startTime);

    // Connect to the HDFS instance and open the desired
    // file:
    // Note: using default,0 as parameters doesn't work
    hdfsFS fs = hdfsConnect("127.0.0.1", 9000);
    hdfsFileInfo *fileInfo = hdfsGetPathInfo(fs, path);

    hdfsFile file = hdfsOpenFile(fs, path, O_RDONLY, 4096, 0, 0);
    EXPECT_NONZERO(file, "hdfsOpenFile")

    // measure latency to open the file
    clock_gettime(CLOCK_MONOTONIC, &openedTime);

    // Try to perform a zero-copy read, if it fails
    // fall back to standard read
    if(force_standard_read || !read_zcr(fs, file, fileInfo, n, buffer_size)) {
        if(force_standard_read) {
            printf("Using standard read\n");
        } else {
            printf("Falling back to standard read\n");
        }

        read_standard(fs, file, fileInfo, n, buffer_size);
    }

    // Measure the final time
    clock_gettime(CLOCK_MONOTONIC, &endTime);

    struct timespec time = timespec_diff(startTime, endTime);
    double speed = (((double)fileInfo[0].mSize)/((double)time.tv_sec + time.tv_nsec/1000000000.0))/(1024.0*1024.0*1024.0);

    printf("Read %f GB with %lfGB/s with a buffer size of %lu\n", ((double)fileInfo->mSize)/(1024.0*1024.0*1024.0), speed, buffer_size);

    struct hdfsReadStatistics *stats;
    hdfsFileGetReadStatistics(file, &stats);
    printf("Statistics:\n\tTotal: %lu\n\tLocal: %lu\n\tShort Circuit: %lu\n\tZero Read: %lu\n", stats->totalBytesRead, stats->totalLocalBytesRead, stats->totalShortCircuitBytesRead, stats->totalZeroCopyBytesRead);
    hdfsFileFreeReadStatistics(stats);

    hdfsCloseFile(fs, file);

    printf("%f", speed);

    return 0;
}