#include "db/password.h"

#include <gtest/gtest.h>

#include <string>

namespace {

using rrs::db::hash_password;
using rrs::db::verify_password;

TEST(Password, VerifiesTheHashItProduced) {
    const std::string stored = hash_password("correct horse");

    EXPECT_TRUE(verify_password("correct horse", stored));
}

TEST(Password, RejectsTheWrongPassword) {
    const std::string stored = hash_password("correct horse");

    EXPECT_FALSE(verify_password("correct horst", stored));
    EXPECT_FALSE(verify_password("", stored));
}

TEST(Password, HashingTheSamePasswordTwiceGivesDifferentHashes) {
    // Salted, so a stolen table cannot be attacked one entry at a time.
    const std::string first = hash_password("correct horse");
    const std::string second = hash_password("correct horse");

    EXPECT_NE(first, second);
    EXPECT_TRUE(verify_password("correct horse", first));
    EXPECT_TRUE(verify_password("correct horse", second));
}

TEST(Password, StoresNothingResemblingThePlaintext) {
    const std::string stored = hash_password("correct horse");

    EXPECT_EQ(stored.find("correct horse"), std::string::npos);
}

TEST(Password, RejectsAStoredValueThatIsNotAHash) {
    // A truncated or hand-edited column must fail closed, not throw.
    EXPECT_FALSE(verify_password("correct horse", ""));
    EXPECT_FALSE(verify_password("correct horse", "not-a-hash"));
    EXPECT_FALSE(verify_password("correct horse", "$argon2id$v=19$m=1,t=1,p=1$aaaa"));
}

TEST(Password, HandlesThePasswordLengthTheProtocolAllows) {
    // docs/PROTOCOL.md caps a password at 128 bytes.
    const std::string longest(128, 'p');
    const std::string stored = hash_password(longest);

    EXPECT_TRUE(verify_password(longest, stored));
    EXPECT_FALSE(verify_password(longest.substr(0, 127), stored));
}

TEST(Password, HandlesAnEmptyPassword) {
    const std::string stored = hash_password("");

    EXPECT_TRUE(verify_password("", stored));
    EXPECT_FALSE(verify_password("x", stored));
}

}  // namespace
