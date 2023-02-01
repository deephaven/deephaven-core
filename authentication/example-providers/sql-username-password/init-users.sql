CREATE SCHEMA deephaven_username_password_auth;

CREATE TABLE deephaven_username_password_auth.users (
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    rounds INT NOT NULL
);

INSERT INTO deephaven_username_password_auth.users (username, password_hash, rounds)
VALUES ('admin', '$2a$10$kjbt1Fq4k4W6EB67GDhAauuIWeI8ppx2gsi6.zLL2R5UYokek8nqO', 10);