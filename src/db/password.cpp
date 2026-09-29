#include "db/password.h"

#include <sodium.h>

#include <mutex>
#include <stdexcept>

namespace rrs::db {
namespace {

// Tuned for interactive logins, which is what these are: roughly 0.1 seconds
// and 64 MiB per hash. The memory is transient but it is per worker, so a pool
// of N workers can be hashing N times that at once.
constexpr unsigned long long kOpsLimit = crypto_pwhash_OPSLIMIT_INTERACTIVE;
constexpr std::size_t kMemLimit = crypto_pwhash_MEMLIMIT_INTERACTIVE;

// Several workers reach this at once on the first logins after startup.
void ensure_initialised() {
    static std::once_flag once;
    std::call_once(once, [] {
        if (::sodium_init() < 0) {
            throw std::runtime_error("libsodium failed to initialise");
        }
    });
}

}  // namespace

std::string hash_password(std::string_view password) {
    ensure_initialised();

    char hashed[crypto_pwhash_STRBYTES];
    if (::crypto_pwhash_str(hashed, password.data(), password.size(), kOpsLimit,
                            kMemLimit) != 0) {
        // The only documented failure is running out of memory.
        throw std::runtime_error("password hashing ran out of memory");
    }

    return std::string(hashed);
}

bool verify_password(std::string_view password, const std::string& stored_hash) {
    ensure_initialised();

    return ::crypto_pwhash_str_verify(stored_hash.c_str(), password.data(),
                                      password.size()) == 0;
}

}  // namespace rrs::db
