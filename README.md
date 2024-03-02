# Detect issues in your PostgreSQL database

## Usage

1. Initialize monitoring, e.g.

`$ bin/pgmaven --username <user> --host <host> --dbname <dbname> --command MonitorInitialize`

2. Run Agent and collect data at some reasonable frequency (e.g. 1 hour)

`$ bin/pgagent --username <user> --host <host> --dbname <dbname> --frequency 1h`

3. Allow to collect data for some period, note if looking at UnusedIndexes the period should encompass all use cases, e.g. end of month processing
4. Issue commands - e.g. see DuplicateIndex example below

## Notes
- Clean up Duplicate Indexes before reviewing Unused Indexes

## Commands

|Command|Description|
|-------|-----------|
|CreateTables|Create tables required for tracking activity over time|
|Exec|Execute SQL statement across all DBs provided|
|Help|Output usage|
|MonitorInitialize|Initialize infrastructure for activity monitoring|
|MonitorReset|Reset activity monitoring data|
|MonitorTerminate|Delete infrastructure for activity monitoring|
|NewActivity|Output New Queries in the specified duration|
|QueryRow|Query (single row) to execute across all DBs provided|
|QueryRows|Query (multiple rows) to execute across all DBs provided|
|Snapshot|Snapshot statistics tables (typically performed by agent)|
|Summary|Status summary|

### Examples

`$ bin/pgmaven --dbname demo --command 'QueryRow:select count(*) from bookings'`

`$ bin/pgmaven --dbname demo --command 'QueryRows:SELECT table_name FROM information_schema.tables'`

`$ bin/pgmaven --dbname demo --command 'QueryRows:!complexQuery.sql`

`$ bin/pgmaven --dbname demo --command NewActivity --duration 24h`

## Issues

|Issue|Description|
|-----|-----------|
|All|Execute all|
|Help|Output usage|
|IndexIssues|Analyze indexes for issues|
|Queries|Report queries with significant impact on the system|
|TableIssues|Analyze tables for issues|

### Index Issues Detected
 - IndexBloat - Index is bloated, should it be reindexed?
 - IndexDuplicate - Duplicate index, one of the pair should be dropped
 - IndexHighWriteLargeNonBtree
 - IndexLowScansHighWrites
 - IndexSeldomUsedLarge - Index is seldom used and on a large table, is it warranted?
 - IndexSmall - Index is on a small table, is it productive?
 - IndexUnused - Index is unused, should it be dropped?

### Table Issues Detected
 - TableGrowth
 - TableSizeLarge

### Examples

`$ bin/pgmaven --dbname demo --detect IndexIssues:IndexDuplicate`

    ISSUE: IndexDuplicate
    SEVERITY: HIGH
    TARGET: boarding_passes_pkey
    DETAIL:
    	Table: boarding_passes, Index Size: 614 MB, Duplicate indexes (boarding_passes_pkey, silly_key)
    	First Index: 'CREATE UNIQUE INDEX boarding_passes_pkey ON bookings.boarding_passes USING btree (ticket_no, flight_id)'
    	Second Index: 'CREATE UNIQUE INDEX silly_key ON bookings.boarding_passes USING btree (ticket_no, flight_id)'
    SUGGESTION:
    	DROP INDEX silly_key

`$ bin/pgmaven --dbname demo --detect TableIssues`

    ISSUE: TableGrowth
    SEVERITY: MEDIUM
    TARGET: boarding_passes
    DETAIL:
            Table: action, current rows: 374983, is growing at 3.15% per day
    SUGGESTION:
            REVIEW table - consider partitioning and/or pruning

`$ bin/pgmaven --dbname demo --detect Queries --duration 24h`


## Building

`$ go build -o bin/pgmaven cmd/pgmaven/*.go`

`go build -o bin/pgagent cmd/pgagent/*.go`

`go list -m -u all`
