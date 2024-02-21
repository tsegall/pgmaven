
## Changes ##

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
