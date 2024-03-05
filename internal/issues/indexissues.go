package issues

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
)

type index struct {
	tableName  string
	indexName  string
	indexCols  string
	isUnique   bool
	size       string
	sizeBytes  int64
	definition string
	scans      int64
	dropped    bool
}

type IndexIssues struct {
	datasource    *dbutils.DataSource
	issues        []utils.Issue
	timing        utils.Timing
	specificIssue string
	tableSizes    map[string]int64
	indexes       map[string]*index
}

const smallTable int64 = 100

func (d *IndexIssues) Init(context utils.Context, ds *dbutils.DataSource) {
	d.datasource = ds
}

// Search for index-related issues.  Optional arg if provided will constrain to only looking for specific issue.
func (d *IndexIssues) Execute(args ...string) {
	startMS := time.Now().UnixMilli()
	d.issues = make([]utils.Issue, 0)

	if len(args) != 0 {
		d.specificIssue = args[0]
	}

	if d.isIssueEnabled("IndexDuplicate") || d.isIssueEnabled("IndexSmall") || d.isIssueEnabled("IndexBloat") || d.isIssueEnabled("IndexOverlapping") {
		if d.isIssueEnabled("IndexBloat") {
			d.doIndexBloat()
		}

		if d.isIssueEnabled("IndexDuplicate") {
			d.doDuplicate()
		}

		if d.isIssueEnabled("IndexOverlapping") {
			d.doOverlapping()
		}

		if d.isIssueEnabled("IndexSmall") {
			d.doSmall()
		}

		if d.specificIssueEnabled() {
			d.timing.SetDurationMS(time.Now().UnixMilli() - startMS)

			return
		}
	}

	indexIssueQuery := `
	WITH table_scans as (
		SELECT relid,
			tables.idx_scan + tables.seq_scan as all_scans,
			( tables.n_tup_ins + tables.n_tup_upd + tables.n_tup_del ) as writes,
					pg_relation_size(relid) as table_size
			FROM pg_stat_user_tables as tables
	),
	all_writes as (
		SELECT sum(writes) as total_writes
		FROM table_scans
	),
	indexes as (
		SELECT idx_stat.relid, idx_stat.indexrelid,
			idx_stat.schemaname, idx_stat.relname as tablename,
			idx_stat.indexrelname as indexname,
			idx_stat.idx_scan,
			pg_relation_size(idx_stat.indexrelid) as index_bytes,
			indexdef ~* 'USING btree' AS idx_is_btree,
			indexdef
		FROM pg_stat_user_indexes as idx_stat
			JOIN pg_index
				USING (indexrelid)
			JOIN pg_indexes as indexes
				ON idx_stat.schemaname = indexes.schemaname
					AND idx_stat.relname = indexes.tablename
					AND idx_stat.indexrelname = indexes.indexname
		WHERE pg_index.indisunique = false
			AND 0 <>ALL (indkey)                 -- no index column is an expression
			AND idx_stat.indexrelname NOT LIKE 'pgmaven_%'
			AND NOT EXISTS                         -- does not enforce a constraint
			(SELECT 1 FROM pg_catalog.pg_constraint c
				WHERE c.conindid = idx_stat.indexrelid)
			AND NOT EXISTS                         -- is not an index partition
			(SELECT 1 FROM pg_catalog.pg_inherits AS inh
				WHERE inh.inhrelid = idx_stat.indexrelid)
	),
	index_ratios AS (
	SELECT schemaname, tablename, indexname,
		idx_scan, all_scans,
		round(( CASE WHEN all_scans = 0 THEN 0.0::NUMERIC
			ELSE idx_scan::NUMERIC/all_scans * 100 END),2) as index_scan_pct,
		writes,
		round((CASE WHEN writes = 0 THEN idx_scan::NUMERIC ELSE idx_scan::NUMERIC/writes END),2)
			as scans_per_write,
		pg_size_pretty(index_bytes) as index_size,
		pg_size_pretty(table_size) as table_size,
		idx_is_btree, index_bytes, indexdef
		FROM indexes
		JOIN table_scans
		USING (relid)
	),
	index_groups AS (
	SELECT 'IndexUnused' as reason, *, 1 as grp
	FROM index_ratios
	WHERE
		idx_scan = 0
		and idx_is_btree
	UNION ALL
	SELECT 'IndexLowScansHighWrites' as reason, *, 2 as grp
	FROM index_ratios
	WHERE
		scans_per_write <= 1
		and index_scan_pct < 10
		and idx_scan > 0
		and writes > 100
		and idx_is_btree
	UNION ALL
	SELECT 'IndexSeldomUsedLarge' as reason, *, 3 as grp
	FROM index_ratios
	WHERE
		index_scan_pct < 5
		and scans_per_write > 1
		and idx_scan > 0
		and idx_is_btree
		and index_bytes > 100000000
	UNION ALL
	SELECT 'IndexHighWriteLargeNonBtree' as reason, index_ratios.*, 4 as grp
	FROM index_ratios, all_writes
	WHERE
		( writes::NUMERIC / ( total_writes + 1 ) ) > 0.02
		AND NOT idx_is_btree
		AND index_bytes > 100000000
	ORDER BY grp, index_bytes DESC )
	SELECT reason, schemaname, tablename, indexname,
		index_scan_pct, scans_per_write, index_size, table_size, indexdef
	FROM index_groups
	`
	err := d.datasource.ExecuteQueryRows(indexIssueQuery, nil, indexProcessor, d)
	if err != nil {
		log.Printf("ERROR: Database: %s, indexIssueQuery failed with error: %v\n", d.datasource.GetDBName(), err)
	}

	d.timing.SetDurationMS(time.Now().UnixMilli() - startMS)
}

// func quote(s string) string {
// 	return "\"" + s + "\""
// }

func (d *IndexIssues) specificIssueEnabled() bool {
	return d.specificIssue != ""
}

func (d *IndexIssues) isIssueEnabled(issue string) bool {
	if d.specificIssue == "" {
		return true
	}

	return issue == d.specificIssue
}

// indexProcessor is invoked for every row of the Index issue query.
// The Query returns a row with the following format (schemaname, tablename, indexname, index_size)
func indexProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}, self any) {
	d := self.(*IndexIssues)
	indexIssue := (*values[0].(*interface{})).(string)
	tableName := string((*values[2].(*interface{})).([]uint8))
	indexName := string((*values[3].(*interface{})).([]uint8))
	indexSize := (*values[6].(*interface{})).(string)
	tableSize := (*values[7].(*interface{})).(string)
	indexDefinition := (*values[8].(*interface{})).(string)

	if d.isIssueEnabled(indexIssue) {
		tableDetail := fmt.Sprintf("Table: %s, Index Size: %s, Table Size: %s, Unused indexes (%s)\n", tableName, indexSize, tableSize, indexName)
		indexDetail := fmt.Sprintf("Index definition: '%s'\n", indexDefinition)
		var solution string
		if indexIssue != "IndexHighWriteLargeNonBtree" {
			solution = fmt.Sprintf("DROP INDEX \"%s\"\n", indexName)
		} else {
			solution = "NONE proposed\n"
		}

		d.issues = append(d.issues, utils.Issue{IssueType: indexIssue, Target: indexName, Severity: utils.High,
			Detail: tableDetail + indexDetail, Solution: solution})
	}
}

func (d *IndexIssues) doDuplicate() {
	duplicateIndexQuery := `
	SELECT table_name, pg_size_pretty(sum(pg_relation_size(idx))::bigint) as size,
		(array_agg(idx))[1] as idx1, (array_agg(idx))[2] as idx2,
		(array_agg(idx))[3] as idx3, (array_agg(idx))[4] as idx4
	FROM (
	SELECT indexrelid::regclass as idx, indrelid::regclass as table_name, (indrelid::text ||E'\n'|| indclass::text ||E'\n'|| indkey::text ||E'\n'||
										coalesce(indexprs::text,'')||E'\n' || coalesce(indpred::text,'')) as key
	FROM pg_index) sub
	GROUP BY table_name, key HAVING count(*)>1
	ORDER BY sum(pg_relation_size(idx)) DESC;
	`
	err := d.datasource.ExecuteQueryRows(duplicateIndexQuery, nil, duplicateIndexProcessor, d)
	if err != nil {
		log.Printf("ERROR: Database: %s, DuplicateIndexQuery failed with error: %v\n", d.datasource.GetDBName(), err)
	}
}

// duplicateIndexProcess is invoked for every row of the Duplicate Index Query.
// The Query returns a row with the following format (tableName, index size, index1, index2) - where index1 and index2 are duplicated.
func duplicateIndexProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}, self any) {
	d := self.(*IndexIssues)
	tableName := string((*values[0].(*interface{})).([]uint8))
	indexSize := (*values[1].(*interface{})).(string)
	index1 := string((*values[2].(*interface{})).([]uint8))
	index2 := string((*values[3].(*interface{})).([]uint8))

	tableDetail := fmt.Sprintf("Table: %s, Index Size: %s, Duplicate indexes (%s, %s)\n", tableName, indexSize, index1, index2)
	index1Definition := d.datasource.IndexDefinition(index1)
	index2Definition := d.datasource.IndexDefinition(index2)
	indexDetail := fmt.Sprintf("First Index: '%s'\nSecond Index: '%s'\n", index1Definition, index2Definition)

	// If Index 2 is unique then kill Index 1
	if strings.Contains(index2Definition, " UNIQUE ") {
		d.issues = append(d.issues, utils.Issue{IssueType: "IndexDuplicate", Target: index1, Severity: utils.High, Detail: tableDetail + indexDetail, Solution: fmt.Sprintf("DROP INDEX %s\n", index1)})
		return
	}

	d.issues = append(d.issues, utils.Issue{IssueType: "IndexDuplicate", Target: index2, Severity: utils.High, Detail: tableDetail + indexDetail, Solution: fmt.Sprintf("DROP INDEX %s\n", index2)})
}

func (d *IndexIssues) doSmall() {
	d.tableSizes = make(map[string]int64)

	tableQuery := `
	select
	sub.table_name
from
	(
	select
		table_name
	from
		information_schema.tables
	where
		table_schema = $1
		and table_type = 'BASE TABLE'
		and table_name not ilike 'PGMAVEN_%'
except
	select
		table_name
	from
		information_schema.tables,
		pg_stat_user_tables psut
	where
		table_name = relname
		and table_schema = $1
		and table_type = 'BASE TABLE'
		and table_name not ilike 'PGMAVEN_%'
		and psut.last_analyze is not null
		and n_live_tup > $2
) as sub
order by
	table_name`

	err := d.datasource.ExecuteQueryRows(tableQuery, []any{d.datasource.GetSchema(), smallTable}, smallTableProcessor, d)
	if err != nil {
		log.Printf("ERROR: Database: %s, Table query failed, error: %v\n", d.datasource.GetDBName(), err)
	}

	d.doSmallCheck()
}

func smallTableProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}, self any) {
	d := self.(*IndexIssues)
	tableName := string((*values[0].(*interface{})).([]uint8))

	query := fmt.Sprintf(`select count(*) from %s`, tableName)
	rows, err := d.datasource.ExecuteQueryRow(query, nil)
	if err != nil {
		log.Printf("ERROR: Database: %s, Query '%s' failed, error: %v\n", d.datasource.GetDBName(), query, err)
	}

	d.tableSizes[tableName] = rows.(int64)
}

func (d *IndexIssues) doSmallCheck() {
	var inClause strings.Builder

	for tableName, value := range d.tableSizes {
		if value < smallTable {
			if inClause.Len() != 0 {
				inClause.WriteString(", ")
			}
			inClause.WriteRune('\'')
			inClause.WriteString(tableName)
			inClause.WriteRune('\'')
		} else {
			d.issues = append(d.issues, utils.Issue{IssueType: "AnalyzeSuggested", Target: tableName, Detail: "n_live_tup < row count\n", Solution: fmt.Sprintf("ANALYZE \"%s\"\n", tableName)})
		}
	}

	smallIndexTemplate := `
		SELECT
			stat.schemaname,
			stat.relname AS tablename,
			stat.indexrelname AS indexname,
			pg_relation_size(stat.indexrelid) AS index_size,
			indexdef
		  FROM pg_catalog.pg_stat_user_indexes stat
		  JOIN pg_catalog.pg_index i using (indexrelid)
		  JOIN pg_catalog.pg_indexes i2 ON stat.schemaname = i2.schemaname AND stat.relname = i2.tablename AND stat.indexrelname = i2.indexname
		  WHERE stat.schemaname = $1 and stat.relname in (%s)
		  AND stat.idx_scan != 0                 -- has been used (unused will be be picked up separately)
		  AND i2.indexdef like '%%USING btree%%'   -- only want BTREE indexes
		  AND 0 <>ALL (i.indkey)                 -- no index column is an expression
		  AND NOT i.indisunique                  -- is not a UNIQUE index
		  AND NOT EXISTS                         -- does not enforce a constraint
			(SELECT 1 FROM pg_catalog.pg_constraint c
			 WHERE c.conindid = stat.indexrelid)
		  AND NOT EXISTS                         -- is not an index partition
			(SELECT 1 FROM pg_catalog.pg_inherits AS inh
			 WHERE inh.inhrelid = stat.indexrelid)
		  ORDER by tablename asc, indexname asc;
		`
	smallIndexQuery := fmt.Sprintf(smallIndexTemplate, inClause.String())
	err := d.datasource.ExecuteQueryRows(smallIndexQuery, []any{d.datasource.GetSchema()}, smallIndexProcessor, d)
	if err != nil {
		log.Printf("ERROR: Database: %s, SmallIndexQuery failed with error: %v\n", d.datasource.GetDBName(), err)
	}
}

func smallIndexProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}, self any) {
	d := self.(*IndexIssues)
	tableName := string((*values[1].(*interface{})).([]uint8))
	indexName := string((*values[2].(*interface{})).([]uint8))
	indexSize := (*values[3].(*interface{})).(int64)
	indexDefinition := (*values[4].(*interface{})).(string)

	tableDetail := fmt.Sprintf("Table: %s, Rows: %d, Index Size: %d, Small indexes (%s)\n", tableName, d.tableSizes[tableName], indexSize, indexName)
	indexDetail := fmt.Sprintf("Index definition: '%s'\n", indexDefinition)

	d.issues = append(d.issues, utils.Issue{IssueType: "IndexSmall", Target: indexName, Severity: utils.High, Detail: tableDetail + indexDetail, Solution: fmt.Sprintf("DROP INDEX \"%s\"\n", indexName)})
}

func (d *IndexIssues) doIndexBloat() {
	d.tableSizes = make(map[string]int64)

	indexBloatQuery := `
WITH btree_index_atts AS (
    SELECT nspname, relname, reltuples, relpages, indrelid, relam,
        regexp_split_to_table(indkey::text, ' ')::smallint AS attnum,
        indexrelid as index_oid
    FROM pg_index
    JOIN pg_class ON pg_class.oid=pg_index.indexrelid
    JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
    JOIN pg_am ON pg_class.relam = pg_am.oid
    WHERE pg_am.amname = 'btree'
    ),
index_item_sizes AS (
    SELECT
    i.nspname, i.relname, i.reltuples, i.relpages, i.relam,
    s.starelid, a.attrelid AS table_oid, index_oid,
    current_setting('block_size')::numeric AS bs,
    /* MAXALIGN: 4 on 32bits, 8 on 64bits (and mingw32 ?) */
    CASE
        WHEN version() ~ 'mingw32' OR version() ~ '64-bit' THEN 8
        ELSE 4
    END AS maxalign,
    24 AS pagehdr,
    /* per tuple header: add index_attribute_bm if some cols are null-able */
    CASE WHEN max(coalesce(s.stanullfrac,0)) = 0
        THEN 2
        ELSE 6
    END AS index_tuple_hdr,
    /* data len: we remove null values save space using it fractionnal part from stats */
    sum( (1-coalesce(s.stanullfrac, 0)) * coalesce(s.stawidth, 2048) ) AS nulldatawidth
    FROM pg_attribute AS a
    JOIN pg_statistic AS s ON s.starelid=a.attrelid AND s.staattnum = a.attnum
    JOIN btree_index_atts AS i ON i.indrelid = a.attrelid AND a.attnum = i.attnum
    WHERE a.attnum > 0
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),
index_aligned AS (
    SELECT maxalign, bs, nspname, relname AS index_name, reltuples,
        relpages, relam, table_oid, index_oid,
      ( 2 +
          maxalign - CASE /* Add padding to the index tuple header to align on MAXALIGN */
            WHEN index_tuple_hdr%maxalign = 0 THEN maxalign
            ELSE index_tuple_hdr%maxalign
          END
        + nulldatawidth + maxalign - CASE /* Add padding to the data to align on MAXALIGN */
            WHEN nulldatawidth::integer%maxalign = 0 THEN maxalign
            ELSE nulldatawidth::integer%maxalign
          END
      )::numeric AS nulldatahdrwidth, pagehdr
    FROM index_item_sizes AS s1
),
otta_calc AS (
  SELECT bs, nspname, table_oid, index_oid, index_name, relpages, coalesce(
    ceil((reltuples*(4+nulldatahdrwidth))/(bs-pagehdr::float)) +
      CASE WHEN am.amname IN ('hash','btree') THEN 1 ELSE 0 END , 0 -- btree and hash have a metadata reserved block
    ) AS otta
  FROM index_aligned AS s2
    LEFT JOIN pg_am am ON s2.relam = am.oid
),
raw_bloat AS (
    SELECT current_database() as dbname, nspname, c.relname AS table_name, index_name,
        bs*(sub.relpages)::bigint AS totalbytes,
        CASE
            WHEN sub.relpages <= otta THEN 0
            ELSE bs*(sub.relpages-otta)::bigint END
            AS wastedbytes,
        CASE
            WHEN sub.relpages <= otta
            THEN 0 ELSE bs*(sub.relpages-otta)::bigint * 100 / (bs*(sub.relpages)::bigint) END
            AS realbloat,
        pg_relation_size(sub.table_oid) as table_bytes,
        stat.idx_scan as index_scans
    FROM otta_calc AS sub
    JOIN pg_class AS c ON c.oid=sub.table_oid
    JOIN pg_stat_user_indexes AS stat ON sub.index_oid = stat.indexrelid
)
SELECT nspname as schema_name, table_name, index_name,
        round(realbloat, 1) as bloat_pct,
        wastedbytes as bloat_bytes, pg_size_pretty(wastedbytes::bigint) as bloat_size,
        totalbytes as index_bytes, pg_size_pretty(totalbytes::bigint) as index_size,
        table_bytes, pg_size_pretty(table_bytes) as table_size,
        index_scans
FROM raw_bloat
WHERE ( realbloat > 50 and wastedbytes > 50000000 )
ORDER BY wastedbytes DESC;`

	err := d.datasource.ExecuteQueryRows(indexBloatQuery, nil, bloatProcessor, d)
	if err != nil {
		log.Printf("ERROR: Database: %s, Bloat query failed with error: %v\n", d.datasource.GetDBName(), err)
	}
}

func bloatProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}, self any) {
	d := self.(*IndexIssues)
	tableName := string((*values[1].(*interface{})).([]uint8))
	indexName := string((*values[2].(*interface{})).([]uint8))
	bloatPercent := string((*values[3].(*interface{})).([]uint8))
	bloatSize := (*values[5].(*interface{})).(string)
	indexSize := (*values[7].(*interface{})).(string)
	tableSize := (*values[9].(*interface{})).(string)
	indexScans := (*values[10].(*interface{})).(int64)

	detail := fmt.Sprintf("Table: %s, Size: %s, Index: '%s', Size: %s, Bloat: %s%%, Bloat Size: %s, Scans: %d)\n",
		tableName, tableSize, indexName, indexSize, bloatPercent, bloatSize, indexScans)

	d.issues = append(d.issues, utils.Issue{IssueType: "IndexBloat", Target: indexName, Severity: utils.High, Detail: detail,
		Solution: fmt.Sprintf("REINDEX INDEX CONCURRENTLY \"%s\"\n", indexName)})
}

func (d *IndexIssues) doOverlapping() {
	d.tableSizes = make(map[string]int64)
	d.indexes = make(map[string]*index)

	indexOverlappingQuery := `
	WITH index_cols_ord as (
		SELECT attrelid, attnum, attname
		FROM pg_attribute
			JOIN pg_index ON indexrelid = attrelid
		WHERE indkey[0] > 0
		ORDER BY attrelid, attnum
	),
	index_col_list AS (
		SELECT attrelid,
			array_agg(attname) as cols
		FROM index_cols_ord
		GROUP BY attrelid
	),
	dup_natts AS (
	SELECT indrelid, indexrelid, indisunique
	FROM pg_index as ind
	WHERE EXISTS ( SELECT 1
		FROM pg_index as ind2
		WHERE ind.indrelid = ind2.indrelid
		AND ( ind.indkey @> ind2.indkey
		 OR ind.indkey <@ ind2.indkey )
		AND ind.indkey[0] = ind2.indkey[0]
		AND ind.indkey <> ind2.indkey
		AND ind.indexrelid <> ind2.indexrelid
	) )
	SELECT userdex.schemaname as schema_name,
		userdex.relname as table_name,
		userdex.indexrelname as index_name,
		array_to_string(cols, ', ') as index_cols,
		dup_natts.indisunique,
		pg_relation_size(dup_natts.indexrelid) as index_size_bytes,
		pg_size_pretty(pg_relation_size(dup_natts.indexrelid)) as index_size,
		indexdef,
		idx_scan as index_scans
	FROM pg_stat_user_indexes as userdex
		JOIN index_col_list ON index_col_list.attrelid = userdex.indexrelid
		JOIN dup_natts ON userdex.indexrelid = dup_natts.indexrelid
		JOIN pg_indexes ON userdex.schemaname = pg_indexes.schemaname
			AND userdex.indexrelname = pg_indexes.indexname
	ORDER BY userdex.schemaname, userdex.relname, cols, userdex.indexrelname;
	`

	err := d.datasource.ExecuteQueryRows(indexOverlappingQuery, nil, overlappingProcessor, d)
	if err != nil {
		log.Printf("ERROR: Database: %s, Overlapping index query failed with error: %v\n", d.datasource.GetDBName(), err)
	}
}

func (d *IndexIssues) findOverlapper(tableName string, indexCols string) (found bool, superceded *index) {
	cols := strings.Split(indexCols, ", ")

	key := tableName + "###"
	for i := 0; i < len(cols); i++ {
		if i != 0 {
			key += ", "
		}
		key += cols[i]
		val, ok := d.indexes[key]
		// If the key exists
		if ok {
			return true, val
		}
	}

	return false, nil

}

func overlappingProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}, self any) {
	d := self.(*IndexIssues)
	tableName := string((*values[1].(*interface{})).([]uint8))
	indexName := string((*values[2].(*interface{})).([]uint8))
	indexCols := (*values[3].(*interface{})).(string)
	isUnique := (*values[4].(*interface{})).(bool)
	indexSizeBytes := (*values[5].(*interface{})).(int64)
	indexSize := (*values[6].(*interface{})).(string)
	indexDefinition := (*values[7].(*interface{})).(string)
	indexScans := (*values[8].(*interface{})).(int64)

	found, superceded := d.findOverlapper(tableName, indexCols)
	if !found {
		d.indexes[tableName+"###"+indexCols] = &index{tableName, indexName, indexCols, isUnique, indexSize, indexSizeBytes, indexDefinition, indexScans, false}
		return
	}

	if !superceded.isUnique && !superceded.dropped {
		supercededDetail := fmt.Sprintf("Table: %s, Index: '%s', Size: %s, Cols: '%s', IsUnique: %t, Scans: %d\nIndex Definition: '%s'\n",
			tableName, superceded.indexName, superceded.size, superceded.indexCols, superceded.isUnique, superceded.scans, superceded.definition)
		replacedDetail := fmt.Sprintf("Replaced by '%s', index on '%s', Size: %s, Scans: %d\nIndex Definition: '%s'\n",
			indexName, indexCols, indexSize, indexScans, indexDefinition)

		replacementUtilization := float32(indexScans*100) / float32(indexScans+superceded.scans)
		replacementRelativeSize := float32(indexSizeBytes) / float32(superceded.sizeBytes)

		var note string
		if !isUnique && replacementRelativeSize > 1.5 && replacementUtilization < .1 {
			note = " -- NOTE: replacement is lightly utilized and significantly larger, consider dropping \"" + indexName + "\" instead"
		}

		solution := fmt.Sprintf("DROP INDEX \"%s\"%s\n", superceded.indexName, note)
		d.issues = append(d.issues, utils.Issue{IssueType: "IndexOverlapping", Target: superceded.indexName, Severity: utils.High,
			Detail: supercededDetail + replacedDetail, Solution: solution})

		// Mark as dropped - so we don't drop it again
		superceded.dropped = true
	}
}

func (d *IndexIssues) GetIssues() []utils.Issue {
	return d.issues
}

func (d *IndexIssues) GetDurationMS() int64 {
	return d.timing.GetDurationMS()
}
