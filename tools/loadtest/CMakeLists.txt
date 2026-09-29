# The load client speaks the same protocol as the server and drives it with the
# same reactor, so it links room_core rather than carrying a second copy of
# either.
#
# It is not part of the deployed image. docker/Dockerfile copies src/ alone and
# turns this off, the same way it does for the tests.
add_executable(room_loadtest
    client.cpp
    main.cpp
    stats.cpp)

target_link_libraries(room_loadtest PRIVATE
    room_core
    rrs_warnings)
