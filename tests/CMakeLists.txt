include(FetchContent)

FetchContent_Declare(googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.15.2
    GIT_SHALLOW TRUE)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(room_server_tests
    acceptor_test.cpp
    buffer_test.cpp
    byte_reader_test.cpp
    database_test.cpp
    event_loop_test.cpp
    frame_test.cpp
    password_test.cpp
    room_test.cpp
    server_test.cpp
    session_test.cpp
    timer_queue_test.cpp
    worker_pool_test.cpp)

target_link_libraries(room_server_tests PRIVATE
    room_core
    GTest::gtest_main
    rrs_warnings)

include(GoogleTest)

# The loop tests wait on real deadlines, so a lost wakeup shows up as a bounded
# failure rather than a build that hangs.
gtest_discover_tests(room_server_tests
    PROPERTIES TIMEOUT 30)
