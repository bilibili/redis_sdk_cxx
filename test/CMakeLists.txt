cmake_minimum_required (VERSION 3.7)

# 项目信息
project (rsdk)

set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} -O0 -Dprivate=public -Dprotected=public -include ${PROJECT_SOURCE_DIR}/sstream_define_as_public.h")
set(CMAKE_CXX_FLAGS "${CMAKE_CPP_FLAGS} -O0 -g --coverage -pipe -Wall -W -fPIC -fstrict-aliasing -Wno-invalid-offsetof -Wno-unused-parameter -fno-omit-frame-pointer -Wno-implicit-fallthrough -faligned-new")
set(CMAKE_C_FLAGS "${CMAKE_CPP_FLAGS} -O0 -g --coverage -pipe -Wall -W -fPIC -fstrict-aliasing -Wno-unused-parameter -fno-omit-frame-pointer -Wno-implicit-fallthrough")

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS http_mgr.proto)

include_directories(
    ${PROJECT_SOURCE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR}
)

file(GLOB ut_api_src "${PROJECT_SOURCE_DIR}/../redis_sdk/src/*.cpp")
set(ut_api_src ${ut_api_src} "${PROJECT_SOURCE_DIR}/test_common.cpp" "${PROJECT_SOURCE_DIR}/mock_node.cpp" ${PROTO_SRCS} )

add_library(api_ut STATIC ${ut_api_src})
target_link_libraries(api_ut)

find_package(GTest CONFIG REQUIRED)

set(test_deps api_ut)
set(test_deps ${test_deps} ${BRPC_LIB})
set(test_deps ${test_deps} ${LEVELDB_LIB})
set(test_deps ${test_deps} ${UUID_LIB})
set(test_deps ${test_deps} ${Protobuf_LIBRARIES})
set(test_deps ${test_deps} glog::glog)
set(test_deps ${test_deps} prometheus-cpp::core)
set(test_deps ${test_deps} gflags)
set(test_deps ${test_deps} fmt)
set(test_deps ${test_deps} xxhash)
set(test_deps ${test_deps} z)
set(test_deps ${test_deps} unwind)
set(test_deps ${test_deps} ssl)
set(test_deps ${test_deps} dl)
set(test_deps ${test_deps} crypto)
set(test_deps ${test_deps} snappy)
set(test_deps ${test_deps} pthread)

set(test_deps ${test_deps} GTest::gtest GTest::gtest_main)

file(GLOB UNITTESTS "*_test.cpp")

foreach(UNITTEST ${UNITTESTS})
    get_filename_component(EXECUTABLE_NAME ${UNITTEST} NAME_WE)
    message(STATUS "add unittest executable ${EXECUTABLE_NAME}")
    add_executable(${EXECUTABLE_NAME} ${UNITTEST})
    target_link_libraries(${EXECUTABLE_NAME} ${test_deps})
    add_test(NAME ${EXECUTABLE_NAME} COMMAND $<TARGET_FILE:${EXECUTABLE_NAME}> WORKING_DIRECTORY ${EXECUTABLE_OUTPUT_PATH})
endforeach(UNITTEST)

