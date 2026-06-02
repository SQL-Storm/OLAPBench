--
-- DuckDB dialect template for dsqgen macros.
-- Mirrors Postgres LIMIT behavior.
--
define __LIMITA = "";
define __LIMITB = "";
define __LIMITC = " limit %d";
define _BEGIN = "-- start query " + [_QUERY] + " in stream " + [_STREAM] + " using template " + [_TEMPLATE];
define _END = "-- end query " + [_QUERY] + " in stream " + [_STREAM] + " using template " + [_TEMPLATE];
