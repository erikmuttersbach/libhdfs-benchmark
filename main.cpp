#include <iostream>

#include <stdio.h>
#include <string.h>

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

int main(int argc, char *argv[]) {
    int n = 1;
    const char *path = "/tmp/100M";
    if(argc == 3) {
        path = argv[1];
        n = atoi(argv[2]);
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
    for(int i=0; i<n; i++) {
        hdfsSeek(fs, file, 0);

        char buffer[4096];
        tSize read = 0;
        do {
            read = hdfsRead(fs, file, buffer, 4096);
            totalRead += read;
        } while (read > 0);
    }

    clock_gettime(CLOCK_MONOTONIC, &endTime);
    struct timespec time = timespec_diff(startTime, endTime);

    // TODO Should we use 2^10 or 1024 for KB, MB, GB?
    printf("Read %f GB in %lf s\n", ((float)totalRead)/(1000.0*1000.0*1000.0), time.tv_sec + time.tv_nsec/1000000000.0);

    hdfsCloseFile(fs, file);
}