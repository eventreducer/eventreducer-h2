CREATE TABLE commands (
  uuid       UUID PRIMARY KEY,
  payload    BLOB,
  hash       BYTEA,
  created_at BIGINT,
  trace      OTHER
);

CREATE INDEX commands_hash_idx ON commands(hash);

CREATE TABLE events (
  uuid       UUID PRIMARY KEY,
  command    UUID REFERENCES commands (uuid),
  payload    BLOB,
  hash       BYTEA,
  created_at BIGINT
);

CREATE INDEX events_command_idx ON events(command);
CREATE INDEX events_hash_idx ON events(hash);