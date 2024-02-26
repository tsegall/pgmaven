# Detect issues in your PostgreSQL database

## Commands

|Command|Description|
|-------|-----------|
|CreateTables|Create tables required for tracking activity over time|
|Exec|Execute SQL statement across all DBs provided|
|Help|Output usage|
|NewActivity|Output New Queries in the specified duration|
|QueryRow|Query (single row) to execute across all DBs provided|
|QueryRows|Query (multiple rows) to execute across all DBs provided|
|ResetIndexData|Reset index data|
|Snapshot|Snapshot statistics tables|
|Summary|Status summary|

### Examples

`$ bin/pgmaven --dbname demo --command 'QueryRow:select count(*) from bookings'`

`$ bin/pgmaven --dbname demo --command 'QueryRows:SELECT table_name FROM information_schema.tables'`

`$ bin/pgmaven --dbname demo --command 'QueryRows:!complexQuery.sql`


## Issues

|Issue|Description|
|-----|-----------|
|All|Execute all|
|DuplicateIndex|Check for duplicate indexes|
|Help|Output usage|
|Queries|Report queries with significant impact on the system|
|SillyIndex|Check for silly indexes|
|TableIssues|Analyze tables for issues|
|UnusedIndex|Check for unused indexes|

### Examples

`$ bin/pgmaven --dbname demo --detect DuplicateIndex`

    ISSUE: DuplicateIndex
    SEVERITY: HIGH
    TARGET: boarding_passes_pkey
    DETAIL:
    	Table: boarding_passes, Index Size: 614 MB, Duplicate indexes (boarding_passes_pkey, silly_key)
    	First Index: 'CREATE UNIQUE INDEX boarding_passes_pkey ON bookings.boarding_passes USING btree (ticket_no, flight_id)'
    	Second Index: 'CREATE UNIQUE INDEX silly_key ON bookings.boarding_passes USING btree (ticket_no, flight_id)'
    SUGGESTION:
    	DROP INDEX silly_key

## Building

`$ go build -o bin/pgmaven cmd/pgmaven/*.go`
`go build -o bin/pgagent cmd/pgagent/*.go`
