cmake_minimum_required(VERSION 2.8.4)
project(clion1)

set ( CMAKE_MODULE_PATH "cmake_modules")


set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES main.cpp)
add_executable(clion1 ${SOURCE_FILES})

find_package (JNI REQUIRED)
if (JNI_FOUND)
    link_directories(${JAVA_JVM_LIBRARY})
    target_link_libraries(clion1 ${JNI_LIBRARIES})
endif (JNI_FOUND)

find_package (libhdfs)
if (LIBHDFS_FOUND)
    include_directories(${LIBHDFS_INCLUDE_DIR})
    target_link_libraries(clion1 hdfs)
endif (LIBHDFS_FOUND)

find_package (libhadoop)
if (LIBHADOOP_FOUND)
    target_link_libraries(clion1 hadoop)
endif (LIBHADOOP_FOUND)