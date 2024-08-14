cmake_minimum_required (VERSION 3.7)

project (redis_sdk)

set (CMAKE_CXX_STANDARD 11)
set (CMAKE_CXX_FLAGS "$ENV{CXXFLAGS} -O2 -g -ggdb -Wall -fno-omit-frame-pointer -Werror=unused-value -Wno-class-memaccess -Wno-aligned-new")

option(WITH_PROMETHEUS "With prometheus" ON)
if (WITH_PROMETHEUS)
    add_definitions(-DWITH_PROMETHEUS)
endif()

find_package(Protobuf REQUIRED)
find_path(FMT_INCLUDE_PATH NAMES fmt/format.h)

include_directories(
    ${FMT_INCLUDE_PATH}
    ${Protobuf_INCLUDE_DIRS}
    ${PROJECT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/redis_sdk/include
    ${CMAKE_CURRENT_BINARY_DIR}
)

aux_source_directory(redis_sdk/src API_SRCS)
add_library(redis_sdk STATIC ${API_SRCS})
set_target_properties(redis_sdk PROPERTIES PUBLIC_HEADER redis_sdk/include/redis_sdk/client.h)

find_package(gflags REQUIRED)
find_package(glog REQUIRED)
find_package(Protobuf REQUIRED)
find_package(leveldb QUIET)
if (${leveldb_FOUND})
    set(LEVELDB_LIB leveldb::leveldb)
else(${leveldb_FOUND})
    find_library(LEVELDB_LIB NAMES leveldb)
    if (NOT LEVELDB_LIB)
        message(FATAL_ERROR "Fail to find leveldb")
    endif()
endif()

find_package(prometheus-cpp REQUIRED)

find_path(BRPC_INCLUDE_PATH NAMES brpc/server.h)
find_library(BRPC_LIB NAMES libbrpc.a brpc)
if ((NOT BRPC_INCLUDE_PATH) OR (NOT BRPC_LIB))
    message(FATAL_ERROR "Fail to find brpc")
endif()


set(EXAMPLE_DEPS
    ${BRPC_LIB}
    ${LEVELDB_LIB}
    ${UUID_LIB}
    ${Protobuf_LIBRARIES}
    ${FMT_LIB}
    glog::glog
    prometheus-cpp::core
    gflags
    fmt
    xxhash

    z
    unwind
    ssl
    dl
    crypto
    snappy
    pthread
)

find_library(TCMALLOC_AND_PROFILER NAMES libtcmalloc_and_profiler.a libtcmalloc_and_profiler)
if (NOT TCMALLOC_AND_PROFILER)
    message(WARNING "TCMALLOC_AND_PROFILER Not Found, Run sudo apt install libgoogle-perftools-dev to install.")
else()
    message(STATUS "Find TCMALLOC_AND_PROFILER")
    set(EXAMPLE_DEPS ${EXAMPLE_DEPS} ${TCMALLOC_AND_PROFILER})
endif()

add_definitions(-DFMT_HEADER_ONLY)

add_executable(client_example redis_sdk/example.cpp)
target_link_libraries(client_example redis_sdk ${EXAMPLE_DEPS})

add_executable(client_tool redis_sdk/client_tool.cpp)
target_link_libraries(client_tool redis_sdk ${EXAMPLE_DEPS})

add_executable(client_bench redis_sdk/client_bench.cpp)
target_link_libraries(client_bench redis_sdk ${EXAMPLE_DEPS})

add_executable(client_async_example redis_sdk/async_example.cpp)
target_link_libraries(client_async_example redis_sdk ${EXAMPLE_DEPS})

install(TARGETS 
    redis_sdk
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    PUBLIC_HEADER DESTINATION include/redis_sdk
)
install(FILES
    redis_sdk/include/redis_sdk/api_status.h
    DESTINATION include/redis_sdk
)
option(API_BUILD_TESTS "build unittest" ON)
if (API_BUILD_TESTS)
    add_subdirectory(test)
endif(API_BUILD_TESTS)
