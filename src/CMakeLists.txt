# Shared server code. main.cpp is deliberately kept out of the library so the
# tests link the same objects the binary does. Sources for db/ join room_core
# as that module is implemented.
add_library(room_core STATIC
    db/connection.cpp
    db/database.cpp
    db/password.cpp
    db/worker_pool.cpp
    net/acceptor.cpp
    net/event_loop.cpp
    net/timer_queue.cpp
    protocol/byte_reader.cpp
    protocol/frame.cpp
    room/room.cpp
    room/room_manager.cpp
    server.cpp
    session/buffer.cpp
    session/session.cpp)

# Headers are included as "net/event_loop.h", rooted at src/.
target_include_directories(room_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# Threads is PUBLIC because std::thread's requirements travel with the header.
# libsodium and libpq stay PRIVATE: their headers appear in no header of ours,
# which is what the forward declarations in connection.h are for.
target_link_libraries(room_core
    PUBLIC Threads::Threads
    PRIVATE PkgConfig::PQ PkgConfig::SODIUM rrs_warnings)

add_executable(room_server
    main.cpp)

target_compile_definitions(room_server PRIVATE
    RRS_VERSION="${PROJECT_VERSION}")

target_link_libraries(room_server PRIVATE
    room_core
    rrs_warnings)
