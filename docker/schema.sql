-- Schema for the realtime room server.
--
-- Loaded into the PostgreSQL container at first start. Kept here rather than
-- under src/ because it is consumed by the database, not by the build.

CREATE TABLE users (
    id            BIGSERIAL PRIMARY KEY,
    -- The protocol caps a username at 32 *bytes* while this caps it at 32
    -- characters, so the column is the looser of the two and can never be the
    -- one to reject a name the server already accepted.
    username      VARCHAR(32) NOT NULL UNIQUE,
    -- Argon2id, salt and parameters included. Never a plaintext password.
    password_hash TEXT        NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE rooms (
    -- Room ids travel as u32 on the wire. BIGINT with a range check is the
    -- honest mapping; INTEGER is signed and would quietly lose the top half.
    id       BIGINT      PRIMARY KEY CHECK (id >= 0 AND id <= 4294967295),
    name     VARCHAR(64) NOT NULL,
    -- JOIN_ROOM_RES reports the member count as u16, so a room that could hold
    -- more than 65535 is a room whose count the protocol cannot state. The
    -- server reads this column into a u16 as well; without the upper bound a
    -- capacity of 70000 would arrive quietly as 4464.
    capacity INTEGER     NOT NULL CHECK (capacity BETWEEN 1 AND 65535)
);

CREATE TABLE chat_messages (
    id      BIGSERIAL     PRIMARY KEY,
    room_id BIGINT        NOT NULL REFERENCES rooms (id),
    user_id BIGINT        NOT NULL REFERENCES users (id),
    -- 1024 bytes on the wire, so 1024 characters here is again the looser cap.
    body    VARCHAR(1024) NOT NULL,
    sent_at TIMESTAMPTZ   NOT NULL
);

-- Chat history is read newest-first for one room at a time.
CREATE INDEX chat_messages_by_room_time ON chat_messages (room_id, sent_at DESC);

-- Rooms are the server's to define, not something clients create by joining an
-- id nobody made. These are the starting set.
INSERT INTO rooms (id, name, capacity) VALUES
    (1, 'lobby', 64),
    (2, 'general', 64),
    (3, 'random', 64);
