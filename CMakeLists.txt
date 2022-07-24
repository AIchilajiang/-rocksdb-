cmake_minimum_required(VERSION 3.0)
project(test)

set(CMAKE_CXX_STANDARD 14)
link_libraries("/usr/lib/librocksdb.so")
link_libraries("/usr/lib/librocksdb.so.5")
link_libraries("/usr/lib/librocksdb.so.5.11")
link_libraries("/usr/lib/librocksdb.so.5.11.3")
include_directories(./rocksdb)


add_executable(test main.cpp)
