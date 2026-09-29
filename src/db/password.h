#pragma once

#include <string>
#include <string_view>

namespace rrs::db {

// Argon2id password hashing by way of libsodium.
//
// Both calls are meant to cost tens of milliseconds and tens of megabytes --
// that is what makes a password hash worth anything -- so they belong on a
// database worker, next to the row being checked, and never on the event loop.
// This is also why they live beside the database code rather than in a module
// of their own.
//
// The returned string carries its own salt and parameters, so verifying needs
// nothing besides the stored value.

// Throws std::runtime_error when libsodium cannot get the memory it wants.
std::string hash_password(std::string_view password);

// False both for a wrong password and for a stored hash this build cannot
// read, which are not worth telling apart at a login prompt.
bool verify_password(std::string_view password, const std::string& stored_hash);

}  // namespace rrs::db
