#pragma once

#include <cstddef>
#include <cstdint>

namespace rrs::protocol {

// Carried in LOGIN_REQ. There is no negotiation: client and server ship
// together, so a mismatch ends the connection rather than starting a
// conversation about it.
inline constexpr std::uint16_t kProtocolVersion = 1;

// Field ceilings, in bytes rather than characters. Exceeding one is bad input
// from an otherwise well-behaved client, so it costs an ERROR_NTF and not the
// connection.
inline constexpr std::size_t kMaxUsernameBytes = 32;
inline constexpr std::size_t kMaxPasswordBytes = 128;
inline constexpr std::size_t kMaxChatTextBytes = 1024;

// Message types are split into bands by direction, so a client forging a
// server-only type is rejected on the band alone, before its payload is
// parsed. See docs/PROTOCOL.md for the catalog and payload layouts.
enum class MessageType : std::uint16_t {
    // Client to server: 0x0001 - 0x7FFF
    kLoginReq = 0x0001,
    kJoinRoomReq = 0x0002,
    kLeaveRoomReq = 0x0003,
    kChatReq = 0x0004,
    kPingReq = 0x0005,

    // Server to client: 0x8000 - 0xFFFF
    kLoginRes = 0x8001,
    kJoinRoomRes = 0x8002,
    kLeaveRoomRes = 0x8003,
    kChatNtf = 0x8004,
    kUserJoinedNtf = 0x8005,
    kUserLeftNtf = 0x8006,
    kPongNtf = 0x8007,
    kErrorNtf = 0x80FF,
};

// True for the client-to-server band. Type 0x0000 belongs to no band and is
// rejected like a server-only type.
constexpr bool is_client_to_server(std::uint16_t type) {
    return type >= 0x0001 && type <= 0x7FFF;
}

// Failures reported to the client through ERROR_NTF. Everything but
// kFieldValidationFailed ends the connection: a peer that breaks the framing
// rules cannot be trusted to keep speaking the protocol, whereas an
// over-length field is just bad input from an otherwise well-behaved client.
enum class ErrorCode : std::uint16_t {
    kMalformedFrame = 1000,
    kUnknownMessageType = 1001,
    kWrongDirection = 1002,
    kFieldValidationFailed = 1003,
    kProtocolVersionMismatch = 1004,

    // Catalogued but never sent: reaching the send buffer limit means the peer
    // is not draining, so the notification would only join the queue that is
    // already too long. Kept here so the number is not handed to something else.
    kSendBufferOverflow = 1005,

    // The server turning a connection away on its own terms rather than on
    // anything the peer did wrong: no room for another connection, or one that
    // never got as far as logging in. One code for both, because a client
    // branches the same way on either -- back off, try again later -- and the
    // message says which it was for whoever is reading.
    kConnectionRefused = 1006,
};

}  // namespace rrs::protocol
