#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <tbb/tbb.h>
#include <tbb/compat/thread>

#ifdef LIBHDFS_HDFS_H
#include <hdfs.h>
#endif

// On Mac OS X clock_gettime is not available
#ifdef __MACH__
#include <mach/mach_time.h>
#define CLOCK_REALTIME 0
#define CLOCK_MONOTONIC 0
int clock_gettime(int clk_id, struct timespec *t){
    mach_timebase_info_data_t timebase;
    mach_timebase_info(&timebase);
    uint64_t time;
    time = mach_absolute_time();
    double nseconds = ((double)time * (double)timebase.numer)/((double)timebase.denom);
    double seconds = ((double)time * (double)timebase.numer)/((double)timebase.denom * 1e9);
    t->tv_sec = seconds;
    t->tv_nsec = nseconds;
    return 0;
}
#else
#include <time.h>
#endif

using namespace std;
using namespace tbb;

timespec timespec_diff(timespec start, timespec end) {
    timespec temp;
    if ((end.tv_nsec - start.tv_nsec) < 0) {
        temp.tv_sec = end.tv_sec - start.tv_sec - 1;
        temp.tv_nsec = 1000000000 + end.tv_nsec - start.tv_nsec;
    } else {
        temp.tv_sec = end.tv_sec - start.tv_sec;
        temp.tv_nsec = end.tv_nsec - start.tv_nsec;
    }
    return temp;
}

#define EXPECT_NONZERO(r, func) if(r==NULL) { \
                                    fprintf(stderr, "%s failed: %s\n", func, strerror(errno)); \
                                    exit(1); \
                                }

void use_data(void *data, size_t length) {
    volatile int c = 0;
    for (size_t i = 0; i < length; i++) {
        c += *((char *) data + i);
    }
}

#ifdef LIBHDFS_HDFS_H
bool read_hdfs_zcr(hdfsFS fs, hdfsFile file, hdfsFileInfo *fileInfo, int n, size_t buffer_size) {
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
        if (rzBuffer != NULL) {
            const void *data = hadoopRzBufferGet(rzBuffer);
            read = hadoopRzBufferLength(rzBuffer);
            total_read += read;

            if (read > 0) {
                use_data((void *) data, read);
            }

            hadoopRzBufferFree(file, rzBuffer);
        }
        else if (errno == EOPNOTSUPP) {
            printf("zcr not supported\n");
            return false;
        }
        else {
            EXPECT_NONZERO(rzBuffer, "hadoopReadZero");
        }
    } while (read > 0);

    if (total_read != fileInfo[0].mSize) {
        fprintf(stderr, "Failed to zero-copy read file to full size\n");
        exit(1);
    }

    hadoopRzOptionsFree(rzOptions);

    return true;
}

bool read_hdfs_standard(hdfsFS fs, hdfsFile file, hdfsFileInfo *fileInfo, int n, size_t buffer_size) {
    task_scheduler_init init(n);
    task_group g;
    for(int i=0; i<n; i++) {
        g.run([&fs, &fileInfo, &file, n, i, buffer_size] {
            size_t chunk_size = (fileInfo[0].mSize/n);
            size_t chunk_offset = i*chunk_size;

            hdfsSeek(fs, file, chunk_offset);

            char *buffer = (char *) malloc(sizeof(char) * buffer_size);
            tSize total_read = 0, read = 0;
            do {
                if(total_read >= chunk_size) {
                    break;
                }

                read = hdfsRead(fs, file, buffer, buffer_size);
                if (read > 0) {
                    use_data(buffer, read);
                }

                total_read += read;
            } while (read > 0);

            if (total_read == 0) {
                fprintf(stderr, "Failed to read any byte from the file\n");
                exit(1);
            }

            free(buffer);
        });
    }

    g.wait();
}
#endif

void read_file(const char *path, size_t file_size, int n, size_t buffer_size) {
    task_scheduler_init init(n);
    task_group g;

    for(int i=0; i<n; i++) {
        g.run([file_size, path, n, i, buffer_size] {
            FILE *file = fopen(path, "r");
            EXPECT_NONZERO(file, "fopen");

            size_t chunk_size = (file_size/n);
            size_t chunk_offset = i*chunk_size;

            fseek(file, chunk_offset, SEEK_SET);

            char *buffer = (char *) malloc(sizeof(char) * buffer_size);
            size_t total_read = 0, read = 0;
            do {
                if(total_read >= chunk_size) {
                    break;
                }

                read = fread(buffer, sizeof(char), buffer_size, file);
                if (read > 0) {
                    use_data(buffer, read);
                }

                total_read += read;
            } while (read > 0);

            if (total_read == 0) {
                fprintf(stderr, "Failed to read any byte from the file\n");
                exit(1);
            }

            free(buffer);
            fclose(file);
        });
    }

    g.wait();
}

void read_file_mmap(const char *path, size_t file_size) {
    int fd = open(path, O_RDONLY);
    //EXPECT_NONZERO(fd, "fopen");

    void *data = mmap(NULL, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if(MAP_FAILED == data) {
        fprintf(stderr, "mmap failed: %s\n", strerror(errno));
        exit(1);
    }

    use_data(data, file_size);

    munmap(data, file_size);
    close(fd);
}

void print_usage() {
    printf("Usage: hdfs_benchmark file_read|file_mmap|hdfs [path]\n");
}

typedef enum {
    hdfs, file_mmap, file_read
} benchmark_type;

/**
* TODO:
*  - get Short circuit reads to work
*  - use readzero reads (mmap)
*  - implement threading
*  Usage:
*/
int main(int argc, char *argv[]) {
    // Setup and parse options
    int n = 1;
    const char *path = "/tmp/1000M";
    size_t buffer_size = 4096;
    bool force_standard_read = false;
    benchmark_type benchmark;
    if (argc < 2) {
        print_usage();
        exit(1);
    }

    if(strcmp(argv[1], "hdfs") == 0) {
        benchmark = benchmark_type::hdfs;
    } else if(strcmp(argv[1], "file_read") == 0) {
        benchmark = benchmark_type::file_read;
    } else if(strcmp(argv[1], "file_mmap") == 0) {
        benchmark = benchmark_type::file_mmap;
    } else {
        print_usage();
        exit(1);
    }

    if (argc >= 5) {
        path = argv[2];
        n = atoi(argv[3]);
        buffer_size = atoi(argv[4]);
    }
    if (argc >= 6) {
        force_standard_read = (strcmp(argv[5], "std") == 0);
    }

    printf("Reading %s with %i threads from %s\n", path, n, benchmark == hdfs ? "HDFS" : (benchmark == file_mmap ? "file mmap" : " file read"));

    struct timespec startTime, openedTime, endTime;
    clock_gettime(CLOCK_MONOTONIC, &startTime);

    size_t file_size = 0;
    if(benchmark == benchmark_type::hdfs) {
#ifdef LIBHDFS_HDFS_H
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
        if (force_standard_read || !read_hdfs_zcr(fs, file, fileInfo, n, buffer_size)) {
            if (force_standard_read) {
                printf("Using standard read\n");
            } else {
                printf("Falling back to standard read\n");
            }

            read_hdfs_standard(fs, file, fileInfo, n, buffer_size);
        }

        file_size = fileInfo[0].mSize;

        struct hdfsReadStatistics *stats;
        hdfsFileGetReadStatistics(file, &stats);
        printf("Statistics:\n\tTotal: %lu\n\tLocal: %lu\n\tShort Circuit: %lu\n\tZero Read: %lu\n", stats->totalBytesRead, stats->totalLocalBytesRead, stats->totalShortCircuitBytesRead, stats->totalZeroCopyBytesRead);
        hdfsFileFreeReadStatistics(stats);

        hdfsCloseFile(fs, file);
#else
        fprintf(stderr, "libhdfs missing\n");
#endif
    } else {
        struct stat file_stat;
        stat(path, &file_stat);
        file_size = file_stat.st_size;

        if(benchmark == benchmark_type::file_read) {
            read_file(path, file_size, n, buffer_size);
        } else {
            read_file_mmap(path, file_size);
        }

    }

    // Measure the final time
    clock_gettime(CLOCK_MONOTONIC, &endTime);
    struct timespec time = timespec_diff(startTime, endTime);
    double speed = (((double) file_size) / ((double) time.tv_sec + time.tv_nsec / 1000000000.0)) / (1024.0 * 1024.0 * 1024.0);

    // Print some text, some parseable results
    printf("Read %f GB with %lfGB/s with a buffer size of %lu\n", ((double) file_size) / (1024.0 * 1024.0 * 1024.0), speed, buffer_size);
    printf("%f\n", speed);

    return 0;
}