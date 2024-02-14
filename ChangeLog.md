
## Changes ##

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
