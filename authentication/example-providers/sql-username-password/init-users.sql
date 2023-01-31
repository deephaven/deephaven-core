CREATE SCHEMA deephaven_username_password_auth;

CREATE TABLE deephaven_username_password_auth.users (
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    rounds INT NOT NULL
);