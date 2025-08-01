cmake_minimum_required(VERSION 3.16)
project(dtest LANGUAGES CXX C)
set(CMAKE_CXX_STANDARD 20)	
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

set(default_build_type "Debug")
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to '${default_build_type}' as none was specified.")
  set(CMAKE_BUILD_TYPE "${default_build_type}" CACHE STRING "Choose the type of build." FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()


find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)

include(ExternalProject)
include(FetchContent)

fetchcontent_declare(
  nlohmann_json
  GIT_REPOSITORY	"https://github.com/jpollack/nlohmann-json"
  GIT_TAG		"as")

set(HDR_HISTOGRAM_BUILD_PROGRAMS OFF CACHE BOOL "")
fetchcontent_declare(
  hdrhistogram
  GIT_REPOSITORY      "https://github.com/HdrHistogram/HdrHistogram_c"
  GIT_TAG                     "0.11.8")
fetchcontent_makeavailable(hdrhistogram)

fetchcontent_makeavailable(nlohmann_json)

add_executable(workload ripemd160.cpp workload.cpp as_proto.cpp util.cpp)
target_link_libraries(workload Threads::Threads nlohmann_json::nlohmann_json)

add_executable(hashtest ripemd160.cpp hashtest.cpp)
target_link_libraries(hashtest)

add_executable(histtest ripemd160.cpp histtest.cpp)
target_link_libraries(histtest PRIVATE hdr_histogram)
target_include_directories(histtest PRIVATE ${hdrhistogram_SOURCE_DIR}/include)

add_executable(latency_recorder latency_recorder.cpp)
target_link_libraries(latency_recorder PRIVATE hdr_histogram)
target_include_directories(latency_recorder PRIVATE ${hdrhistogram_SOURCE_DIR}/include)

add_executable(hdr_decoder hdr_decoder.cpp)
target_link_libraries(hdr_decoder PRIVATE hdr_histogram)
target_include_directories(hdr_decoder PRIVATE ${hdrhistogram_SOURCE_DIR}/include)

add_executable(info ripemd160.cpp info.cpp as_proto.cpp util.cpp)
target_link_libraries(info Threads::Threads nlohmann_json::nlohmann_json)

add_executable(aswire2json ripemd160.cpp aswire2json.cpp as_proto.cpp util.cpp)
target_link_libraries(aswire2json Threads::Threads nlohmann_json::nlohmann_json)

