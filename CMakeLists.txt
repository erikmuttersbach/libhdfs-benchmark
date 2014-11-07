cmake_minimum_required(VERSION 2.8.4)
project(clion1)

set ( CMAKE_MODULE_PATH "./cmake_modules")
link_directories(/usr/lib/jvm/java-7-openjdk-amd64/jre/lib/amd64/server/)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES main.cpp)
add_executable(clion1 ${SOURCE_FILES})
target_link_libraries(clion1 hadoop)
target_link_libraries(clion1 jvm)
target_link_libraries(clion1 tbb)

find_package (hdfs)
if (HDFS_FOUND)
    link_directories(/usr/local/hadoop/lib/native)
    include_directories(/home/erikmuttersbach/Downloads/hadoop-2.5.1-src/hadoop-hdfs-project/hadoop-hdfs/src/main/native/libhdfs)

    target_link_libraries(clion1 hdfs)
endif (HDFS_FOUND)