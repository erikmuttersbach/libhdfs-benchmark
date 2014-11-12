#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <getopt.h>

#ifdef LIBHDFS_FOUND
#include <hdfs.h>
#endif

// On Mac OS X clock_gettime is not available
#ifdef __MACH__
#include <mach/mach_time.h>
//#include <Foundation/Foundation.h>

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

#define EXPECT_NONNEGATIVE(r, func) if(r < 0) { \
                                    fprintf(stderr, "%s failed: %s\n", func, strerror(errno)); \
                                    exit(1); \
                                }

enum benchmark {
    none = 0, hdfs, file_mmap, file_read
};

static const char *benchmark_s[] = {
        "none", "hdfs", "file_mmap", "file_read"
};

struct {
    const char *path = "/tmp/1000M";
    size_t buffer_size = 4096;
    int force_hdfs_standard_read = 0;

    // Linux specific improvements
    int advise_willneed = false;
    int advise_sequential = false;
    int use_readahead = false;
    int use_ioprio = false;

    benchmark benchmark;
} options;

/**
* Makes sure the data is actually "touched", e.g. loaded
* in memory.
*
* @see http://stackoverflow.com/questions/26871638/how-to-prevent-the-compiler-from-optimizing-memory-access-to-benchmark-read-vs?noredirect=1#comment42312199_26871638
*/
uint64_t use_data(void *data, size_t length) {
    // Slower:
    /*
    int c = 0;
    for (size_t i = 0; i < length; i++) {
        if(1 == *((char *) data + i)) {
            c++;
        }
    }
     */
    volatile uint64_t temp = 0;
    uint64_t sum = 0;
    for (size_t i = 0; i < length; i++) {
        sum += *((char *) data + i);
    }
    temp = sum;
    return temp;
}

#ifdef LIBHDFS_HDFS_H
bool read_hdfs_zcr(hdfsFS fs, hdfsFile file, hdfsFileInfo *fileInfo, size_t buffer_size) {
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

bool read_hdfs_standard(hdfsFS fs, hdfsFile file, hdfsFileInfo *fileInfo, size_t buffer_size) {
    char *buffer = (char *) malloc(sizeof(char) * buffer_size);
    tSize total_read = 0, read = 0;
    do {
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
}
#endif

void read_file() {
    FILE *file = fopen(options.path, "r");
    EXPECT_NONZERO(file, "fopen");

#ifdef __linux__
    if(options.advise_willneed || options.advise_sequential) {
        posix_fadvise(fileno(file), 0, file_size, options.advise_sequential ? POSIX_FADV_SEQUENTIAL : POSIX_FADV_WILLNEED);
    }

    if(options.use_readahead) {
        readahead(fileno(file), 0, file_size);
    }

    if(options.use_ioprio) {
        syscall(SYS_ioprio_set, getpid(), 1, 1);
    }
#endif

    char *buffer = (char *) malloc(sizeof(char) * options.buffer_size);
    size_t total_read = 0, read = 0;
    do {
        read = fread(buffer, sizeof(char), options.buffer_size, file);
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
}

void read_file_mmap() {
    int fd = open(options.path, O_RDONLY);
    EXPECT_NONNEGATIVE(fd, "open");

    struct stat file_stat;
    stat(options.path, &file_stat);

    void *data = mmap(NULL, file_stat.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if(MAP_FAILED == data) {
        fprintf(stderr, "mmap failed: %s\n", strerror(errno));
        exit(1);
    }

#ifdef __linux__
    if(options.advise_willneed || options.advise_sequential) {
        posix_madvise(data, file_stat.st_size, options.advise_sequential ? POSIX_MADV_SEQUENTIAL : POSIX_MADV_WILLNEED);
    }

    if(options.use_ioprio) {
        syscall(SYS_ioprio_set, getpid(), 1, 1);
    }
#endif

    use_data(data, file_stat.st_size);

    munmap(data, file_stat.st_size);
    close(fd);
}

void print_usage(int argc, char *argv[]) {
    printf("Usage: %s -t file_read|file_mmap|hdfs\n", argv[0]);
    printf("\t-f FILE\n");
    printf("\t-b BUFFER_SIZE\n");
    printf("\t-t BENCHMARK_TYPE\n");
}

void parse_options(int argc, char *argv[]) {
    static struct option options_config[] = {
            {"force-hdfs-standard", no_argument, &options.force_hdfs_standard_read, 1},
#ifdef __linux__
            {"advise-sequential",   no_argument, &options.advise_sequential, 1},
            {"advise-willneed",     no_argument, &options.advise_willneed, 1},
            {"use-readahead",       no_argument, &options.use_readahead, 1},
            {"use-ioprio",          no_argument, &options.use_ioprio, 1},
#endif
            {"file",    optional_argument, 0, 'f'},
            {"buffer",  optional_argument, 0, 'b'},
            {"type",    required_argument, 0, 't'},
            {0, 0, 0, 0}
    };

    int c = 0;
    while(c >= 0) {
        int option_index;
        c = getopt_long(argc, argv, "f:b:t:", options_config, &option_index);

        switch(c) {
            case 'f':
                options.path = optarg;
                break;
            case 'b':
                options.buffer_size = atoi(optarg);
                break;
            case 't':
                if(strcmp(optarg, "hdfs") == 0) {
                    options.benchmark = benchmark::hdfs;
                } else if(strcmp(optarg, "file_mmap") == 0) {
                    options.benchmark = benchmark::file_mmap;
                } else if(strcmp(optarg, "file_read") == 0) {
                    options.benchmark = benchmark::file_read;
                } else {
                    printf("%s is not a valid benchmark. Options are: hdfs, file_mmap, file_read\n", optarg);
                    exit(1);
                }
                break;
            default:
                break;
        }
    }

    if(options.benchmark == benchmark::none) {
        print_usage(argc, argv);
        printf("Please select a benchmark type\n");
        exit(1);
    }

    if(options.advise_willneed && options.advise_sequential) {
        printf("Options --advise-willneed and --advise-sequential are exclusive\n");
        exit(1);
    }
}

int main(int argc, char *argv[]) {
    parse_options(argc, argv);

    printf("Reading from %s using benchmark %s, buffer size %i, forcing standard HDFS read: %s\n", options.path, benchmark_s[options.benchmark], (int)options.buffer_size, options.force_hdfs_standard_read > 0 ? "yes" : "no");

    // If we are root, we can clear the filesystem
    // cache
#ifdef __linux__
    if(getuid() == 0) {
        system("sudo sync; sudo echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null");
        printf("Cleared filesystem cache\n");
    }
#endif

    struct timespec startTime, openedTime, endTime;
    clock_gettime(CLOCK_MONOTONIC, &startTime);

    size_t file_size = 0;
    if(options.benchmark == benchmark::hdfs) {
#ifdef LIBHDFS_FOUND
        // Connect to the HDFS instance and open the desired
        // file:
        // Note: using default,0 as parameters doesn't work
        hdfsFS fs = hdfsConnect("127.0.0.1", 9000);
        hdfsFileInfo *fileInfo = hdfsGetPathInfo(fs, options.path);

        hdfsFile file = hdfsOpenFile(fs, path, O_RDONLY, 4096, 0, 0);
        EXPECT_NONZERO(file, "hdfsOpenFile")

        // measure latency to open the file
        clock_gettime(CLOCK_MONOTONIC, &openedTime);

        // Try to perform a zero-copy read, if it fails
        // fall back to standard read
        if (force_standard_read || !read_hdfs_zcr(fs, file, fileInfo)) {
            if (options.force_hdfs_standard_read) {
                printf("Using standard read\n");
            } else {
                printf("Falling back to standard read\n");
            }

            read_hdfs_standard(fs, file, fileInfo);
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
        stat(options.path, &file_stat);
        file_size = file_stat.st_size;

        if(options.benchmark == benchmark::file_read) {
            read_file();
        } else {
            read_file_mmap();
        }

    }

    // Measure the final time
    clock_gettime(CLOCK_MONOTONIC, &endTime);
    struct timespec time = timespec_diff(startTime, endTime);
    double speed = (((double) file_size) / ((double) time.tv_sec + time.tv_nsec / 1000000000.0)) / (1024.0 * 1024.0);

    // Print some text, some parseable results
    printf("Read %f MB with %lfMiB/s\n", ((double) file_size) / (1024.0 * 1024.0), speed);
    printf("%f\n", speed);

    return 0;
}
