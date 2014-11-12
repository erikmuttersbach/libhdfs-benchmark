cmake_minimum_required(VERSION 2.8.4)
project(hdfs_benchmark)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake_modules")


set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES main.cpp)
add_executable(hdfs_benchmark ${SOURCE_FILES})

find_package (JNI REQUIRED)
if (JNI_FOUND)
    link_directories(${JAVA_JVM_LIBRARY})
    target_link_libraries(hdfs_benchmark ${JNI_LIBRARIES})
endif (JNI_FOUND)

find_package (libhdfs)
if (LIBHDFS_FOUND)
    include_directories(${LIBHDFS_INCLUDE_DIR})
    target_link_libraries(hdfs_benchmark ${LIBHDFS_LIBRARY})
endif (LIBHDFS_FOUND)

find_package (libhadoop)
if (LIBHADOOP_FOUND)
    target_link_libraries(hdfs_benchmark ${LIBHADOOP_LIBRARY})
endif (LIBHADOOP_FOUND)
