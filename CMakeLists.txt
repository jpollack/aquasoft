cmake_minimum_required(VERSION 3.16)
project(dtest LANGUAGES CXX C)
set(CMAKE_CXX_STANDARD 20)	
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)

include(ExternalProject)
include(FetchContent)

fetchcontent_declare(
  nlohmann_json
  GIT_REPOSITORY	"https://github.com/jpollack/nlohmann-json"
  GIT_TAG		"as")

fetchcontent_makeavailable(nlohmann_json)

add_executable(workload ripemd160.cpp workload.cpp as_proto.cpp util.cpp)
target_link_libraries(workload Threads::Threads nlohmann_json::nlohmann_json)

add_executable(hashtest ripemd160.cpp hashtest.cpp)
target_link_libraries(hashtest)


