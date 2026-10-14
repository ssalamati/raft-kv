cmake_minimum_required(VERSION 3.18)
project(raft_kv VERSION 0.1.0 LANGUAGES CXX)

# ---------------------------------------------------------------------------
# Language / build settings
# ---------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)                 # -> Threads::Threads (pthread)
# Not CONFIG-only: Debian/Ubuntu ship no protobuf config file, only CMake's own
# FindProtobuf module. Both routes define protobuf::libprotobuf and protobuf::protoc.
find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)             # -> gRPC::grpc++, gRPC::grpc_cpp_plugin

# RocksDB ships a CMake package config on most distros/Homebrew; fall back to a
# plain library/header search where it does not.
find_package(RocksDB CONFIG QUIET)             # -> RocksDB::rocksdb
if(NOT TARGET RocksDB::rocksdb)
  find_library(ROCKSDB_LIBRARY NAMES rocksdb REQUIRED)
  find_path(ROCKSDB_INCLUDE_DIR NAMES rocksdb/db.h REQUIRED)
  add_library(RocksDB::rocksdb UNKNOWN IMPORTED)
  set_target_properties(RocksDB::rocksdb PROPERTIES
    IMPORTED_LOCATION             "${ROCKSDB_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${ROCKSDB_INCLUDE_DIR}"
    INTERFACE_LINK_LIBRARIES      Threads::Threads)
endif()

# ---------------------------------------------------------------------------
# Protobuf + gRPC code generation (proto/raft_kv.proto -> build/generated/)
# ---------------------------------------------------------------------------
set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
set(GEN_DIR   ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GEN_DIR})

set(PROTO_SRCS ${GEN_DIR}/raft_kv.pb.cc ${GEN_DIR}/raft_kv.grpc.pb.cc)
set(PROTO_HDRS ${GEN_DIR}/raft_kv.pb.h  ${GEN_DIR}/raft_kv.grpc.pb.h)

add_custom_command(
  OUTPUT  ${PROTO_SRCS} ${PROTO_HDRS}
  COMMAND $<TARGET_FILE:protobuf::protoc>
  ARGS    --proto_path=${PROTO_DIR}
          --cpp_out=${GEN_DIR}
          --grpc_out=${GEN_DIR}
          --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
          ${PROTO_DIR}/raft_kv.proto
  DEPENDS ${PROTO_DIR}/raft_kv.proto
  COMMENT "Generating C++ sources from raft_kv.proto"
  VERBATIM)

add_library(raft_kv_proto STATIC ${PROTO_SRCS} ${PROTO_HDRS})
target_include_directories(raft_kv_proto PUBLIC ${GEN_DIR})
target_link_libraries(raft_kv_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)

# ---------------------------------------------------------------------------
# Executable
# ---------------------------------------------------------------------------
# Raft + storage logic, shared by the server (and by tests, if you add them).
add_library(raft_kv_core STATIC src/storage.cpp src/raft_node.cpp)
target_include_directories(raft_kv_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(raft_kv_core PUBLIC
  raft_kv_proto
  RocksDB::rocksdb
  Threads::Threads)                            # pthread

add_executable(raft_server src/server.cpp)     # one cluster node
target_link_libraries(raft_server PRIVATE raft_kv_core)

add_executable(raft_client src/client.cpp)     # CLI client: only needs the generated stubs
target_link_libraries(raft_client PRIVATE raft_kv_proto)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  foreach(tgt raft_kv_core raft_server raft_client)
    target_compile_options(${tgt} PRIVATE -Wall -Wextra -Wpedantic)
  endforeach()
endif()

install(TARGETS raft_server raft_client RUNTIME DESTINATION bin)
