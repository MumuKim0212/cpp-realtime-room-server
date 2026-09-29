cmake_minimum_required(VERSION 3.20)

project(realtime_room_server
    VERSION 0.1.0
    DESCRIPTION "epoll-based realtime room server"
    LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Emit compile_commands.json for clangd / IDE integration.
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

# The reactor sits directly on epoll, so the build itself only completes on
# Linux. Use Docker (see docker/) on other platforms.
if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(WARNING
        "Building on ${CMAKE_SYSTEM_NAME}. The epoll reactor is Linux-only and "
        "the build will fail; use Docker to build and run the server.")
endif()

option(RRS_BUILD_TESTS "Build unit tests" ON)
option(RRS_BUILD_LOADTEST "Build the load test client" ON)

# The database workers run on their own threads.
find_package(Threads REQUIRED)

# System libraries rather than fetched sources: both ship as distribution
# packages, and building a crypto library from source is a liability, not a
# convenience.
find_package(PkgConfig REQUIRED)
pkg_check_modules(SODIUM REQUIRED IMPORTED_TARGET libsodium)
pkg_check_modules(PQ REQUIRED IMPORTED_TARGET libpq)

# Shared warning flags, consumed by every target we own.
add_library(rrs_warnings INTERFACE)
target_compile_options(rrs_warnings INTERFACE
    $<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall;-Wextra;-Wpedantic>
    $<$<CXX_COMPILER_ID:MSVC>:/W4>)

add_subdirectory(src)

# Both of these live outside src/ and are switched off in the deployed image,
# which copies src/ alone: the tests would pull GoogleTest over the network,
# and nothing in a running container drives load at itself.
if(RRS_BUILD_LOADTEST)
    add_subdirectory(tools/loadtest)
endif()

if(RRS_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
