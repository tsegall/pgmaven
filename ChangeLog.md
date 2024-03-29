
## Changes ##

### 0.22.0
 - ENH: Add Detect/IndexIssues:IndexHighNullPercent - detect indexes that are mostly indexing nulls
 - ENH: Output the size of the Database in Command/Summary

### 0.21.0
 - BUG: Fix tests

### 0.20.0
 - ENH: Add pg_stat_activity to the list of tables that we snapshot (provides number of active connections)
 - ENH: Add Detect/ConfigIssues

### 0.19.0
 - ENH: Add IndexLowCardinalityColumn to Detect/IndexIssues - detect indexes where one column in index has very low cardinality

### 0.18.0
 - ENH: If agent loses connectivity then report once and retry more often until connection established
 - ENH: Add --dryrun option to report what would be done for some commands

### 0.17.0
 - ENH: Add IndexMissing to Detect/IndexIssues

### 0.16.0
 - ENH: Tune Detect/IndexIssues:IndexOverlapping

### 0.15.0
 - ENH: Add TableBloat detection to Detect/TableIssues

### 0.14.0
 - ENH: Add IndexOverlapping detection to Detect/IndexIssues

### 0.13.0
 - ENH: Add IndexBloat detection to Detect/IndexIssues

### 0.12.0
 - ENH: Move index issue detection to IndexIssues (similar to TableIssues)
 - ENH: Support selective issue detection for Detect/TableIssues and Detect/IndexIssues
 - ENH: Broaden out the index issue detections
 - ENH: Improve performance of Detect/IndexIssues

### 0.11.0
 - ENH: Add Command/MonitorTerminate
 - ENH: Rename Command/CreateTables -> Command/MonitorInitialize; Rename Command/ResetIndexData -> Command/MonitorReset

### 0.10.0
 - ENH: Add indexes on insert_dt to pgmaven tables
 - ENH: Detect/All - should print out the Execution time
 - ENH: Honor Postgres environment variables PGDATABASE, PGHOST, PGPASSWORD, PGPORT, PGUSER if set
 - INT: Upgrade dependencies

### 0.9.0
 - ENH: Command/NewActivity - list new queries/new index use in the last Duration/DurationOffset
 - ENH: Detect/Queries - make output more friendly for importing into Excel, also add decoder based on pattern-matching SQL

### 0.8.0
 - ENH: Command/Snapshot - Should print the name of the Database with an error
 - ENH: Command/Exec - Execute SQL non-query command on the Database(s)
 - ENH: Where possible print out the Database name on errors
 - ENH: Command/QueryRows - Support !<filename>
 - BUG: Remove silly default for username
 - BUG: Fix case when password is unset


### 0.7.0
 - ENH: Command/Summary - Output Server Version, Server Start Time, and whether pg_stat_statements are enabled, ensure pgmaven has been initialized
 - ENH: Issues now have an associated severity
 - BUG: --verbose was not honored

### 0.6.0
 - ENH: Flip to Posix flags ("github.com/spf13/pflag") rather than Go default
 - ENH: Unify DB Options
 - ENH: Detector/UnusedIndexes - Order by table, then index + print Table size
 - BUG: Detector/Queries - getClosest() was not using UTC + exclude Explain/Prepare + readable durations (not just ms)

### 0.5.0
 - ENH: Detector/Queries - Print out the Analysis period, report if set has new elements
 - ENH: Comman/QueryRow - Support queries from file - use !
 - BUG: Detector/AllIssues - had wrong name
 - INT: Upgrade dependencies
