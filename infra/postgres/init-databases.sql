-- Runs once, on first postgres container start (docker-entrypoint-initdb.d).
--
-- One physical postgres instance, six LOGICAL databases. Services cannot see
-- each other's tables, so the database-per-service boundary holds - but a
-- laptop only runs one container instead of six. This is a deliberate local
-- development trade-off, not a shortcut in the architecture.

CREATE DATABASE auth_db;
CREATE DATABASE catalog_db;
CREATE DATABASE order_db;
CREATE DATABASE inventory_db;
CREATE DATABASE payment_db;
CREATE DATABASE notification_db;

\connect auth_db
CREATE EXTENSION IF NOT EXISTS citext;      -- case-insensitive email column
CREATE EXTENSION IF NOT EXISTS pgcrypto;

\connect catalog_db
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS pg_trgm;     -- trigram index for product search

\connect order_db
CREATE EXTENSION IF NOT EXISTS pgcrypto;

\connect inventory_db
CREATE EXTENSION IF NOT EXISTS pgcrypto;

\connect payment_db
CREATE EXTENSION IF NOT EXISTS pgcrypto;

\connect notification_db
CREATE EXTENSION IF NOT EXISTS pgcrypto;
