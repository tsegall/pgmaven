package issues

import (
	"database/sql"
	"fmt"
	"log"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"
	"strconv"
	"time"
)

type setting struct {
	value string
	units string
}

type ConfigIssues struct {
	datasource *dbutils.DataSource
	context    utils.Context
	settings   map[string]setting
	issues     []utils.Issue
	timing     utils.Timing
}

func (d *ConfigIssues) Init(context utils.Context, ds *dbutils.DataSource) {
	d.datasource = ds
	d.context = context
	d.settings = make(map[string]setting)
}

// Queries - report queries with significant impact on the system.
func (d *ConfigIssues) Execute(args ...string) {
	startMS := time.Now().UnixMilli()
	d.issues = make([]utils.Issue, 0)

	query := `
SELECT name, setting, unit FROM pg_settings where name in (
	'checkpoint_completion_target',
	'default_statistics_target',
	'effective_cache_size',
	'maintenance_work_mem',
	'max_connections',
	'shared_buffers',
	'work_mem'
)`
	// 'effective_io_concurrency',
	// 'huge_pages',
	// 'min_wal_size',
	// 'max_wal_size',
	// 'max_parallel_workers_per_gather',
	// 'max_parallel_workers',
	// 'max_parallel_maintenance_workers',
	// 'max_worker_processes',
	// 'random_page_cost',
	// 'wal_buffers',

	err := d.datasource.ExecuteQueryRows(query, nil, configIssuesProcessor, d)

	if err != nil {
		fmt.Printf("ERROR: Database: %s, ConfigIssues: failed to get DB settings, error: %v\n", d.datasource.GetDBName(), err)
		return
	}

	d.analyzeSettings()

	d.timing.SetDurationMS(time.Now().UnixMilli() - startMS)
}

var memoryTotal = 128 * 1024 * 1024 * 1024
var memoryBuffers = memoryTotal / (8 * 1024)

func configIssuesProcessor(rowNumber int, columnTypes []*sql.ColumnType, values []interface{}, self any) {
	d := self.(*ConfigIssues)
	name := (*values[0].(*interface{})).(string)
	value := (*values[1].(*interface{})).(string)
	unit := *values[2].(*interface{})
	units := ""
	if unit != nil {
		units = unit.(string)
	}

	d.settings[name] = setting{value, units}
}

func (d *ConfigIssues) analyzeSettings() {
	// Need to check max_connections first - since we are going to use this in other settings calculations
	maxObservedQuery := `select max(cnt) from (select count(*) as cnt, insert_dt from pgmaven_pg_stat_activity where state = 'active' group by insert_dt) as foo`
	maxConnectionsObserved, err := d.datasource.ExecuteQueryRow(maxObservedQuery, nil)
	if err != nil {
		log.Printf("ERROR: Database: %s, Query '%s' failed with error: %v\n", d.datasource.GetDBName(), maxObservedQuery, err)
		return
	}

	name := "max_connections"
	s := d.settings[name]
	maxConnectionsSetting, _ := strconv.Atoi(s.value)
	if maxConnectionsSetting > 200 && maxConnectionsObserved.(int64)*15 < 2000 {
		d.issues = append(d.issues, utils.Issue{IssueType: "Config", Target: name,
			Detail:   fmt.Sprintf("Setting: %s, value: %s - excessively large, maximum observed: %d\n", name, s.value, maxConnectionsObserved),
			Severity: utils.High, Solution: "Update postgresql.conf - 'max_connections = 200'\n"})
		maxConnectionsSetting = 200
	}

	for name, s := range d.settings {
		switch name {
		case "checkpoint_completion_target":
			// Target = .9
			currentValue, _ := strconv.ParseFloat(s.value, 32)
			if currentValue < .85 || currentValue > .95 {
				d.issues = append(d.issues, utils.Issue{IssueType: "Config", Target: name,
					Detail:   fmt.Sprintf("Setting: %s, value(units): %s%s - unusual\n", name, s.value, s.units),
					Severity: utils.High,
					Solution: "Review setting - this is typically 0.9\n"})
			}
		case "default_statistics_target":
			// Target = 100
			currentValue, _ := strconv.ParseInt(s.value, 10, 64)
			if currentValue != 100 {
				d.issues = append(d.issues, utils.Issue{IssueType: "Config", Target: name,
					Detail:   fmt.Sprintf("Setting: %s, value: %s\n", name, s.value),
					Severity: utils.High,
					Solution: "Review setting - this is typically 100\n"})
			}
		case "effective_cache_size":
			// Target = Total RAM * 0.5
			target := float32(memoryTotal) * 0.5
			currentValue, _ := strconv.ParseInt(s.value, 10, 64)
			effectiveCacheSize := utils.PgUnitsToBytes(currentValue, s.units)
			if issue := testRange(effectiveCacheSize, int64(0.8*target), int64(1.2*target)); issue != "" {
				d.issues = append(d.issues, utils.Issue{IssueType: "Config", Target: name,
					Detail: fmt.Sprintf("Setting: %s, value(units): %s %s (%.2fGB) - %s\nGoal: Total RAM * 0.5\n",
						name, s.value, s.units, float32(effectiveCacheSize)/float32(utils.SIZE_GB), issue),
					Severity: utils.High,
					Solution: fmt.Sprintf("Update postgresql.conf - '%s = %s'\n", name, utils.PrettyPrint(utils.CleartoGB(int64(target))))})
			}
		case "maintenance_work_mem":
			// Target = Total RAM * 0.05
			target := float32(memoryTotal) * 0.05
			currentValue, _ := strconv.ParseInt(s.value, 10, 64)
			maintenanceWorkMem := utils.PgUnitsToBytes(currentValue, s.units)
			if issue := testRange(maintenanceWorkMem, int64(0.5*target), int64(1.5*target)); issue != "" {
				d.issues = append(d.issues, utils.Issue{IssueType: "Config", Target: name,
					Detail: fmt.Sprintf("Setting: %s, value(units): %s %s (%.2fMB) - %s\nGoal: Total RAM * 0.05\n",
						name, s.value, s.units, float32(maintenanceWorkMem)/float32(utils.SIZE_MB), issue),
					Severity: utils.High,
					Solution: fmt.Sprintf("Update postgresql.conf - '%s = %s'\n", name, utils.PrettyPrint(utils.CleartoMB(int64(target))))})
			}
		case "shared_buffers":
			// Target = 15% to 25% of the machine’s total RAM
			shared_buffers, _ := strconv.ParseInt(s.value, 10, 64)
			if issue := testRange(shared_buffers, int64(0.15*float32(memoryBuffers)), int64(0.25*float32(memoryBuffers))); issue != "" {
				d.issues = append(d.issues, utils.Issue{IssueType: "Config", Target: name,
					Detail: fmt.Sprintf("Setting: %s, value(units): %s %s (%.2fGB) - %s\nGoal: 15%% to 25%% of the machine’s total RAM\n",
						name, s.value, s.units, float32(utils.PgUnitsToBytes(shared_buffers, s.units))/float32(utils.SIZE_GB), issue),
					Severity: utils.High,
					Solution: fmt.Sprintf("Update postgresql.conf - '%s = %s'\n", name, utils.PrettyPrint(int64(memoryTotal/4)))})
			}
		case "work_mem":
			//	Target = Total RAM * 0.25 / max_connections
			target := (float32(memoryTotal) * 0.25) / float32(maxConnectionsSetting)
			currentValue, _ := strconv.ParseInt(s.value, 10, 64)
			workMem := utils.PgUnitsToBytes(currentValue, s.units)
			if issue := testRange(workMem, int64(0.8*target), int64(1.2*target)); issue != "" {
				d.issues = append(d.issues, utils.Issue{IssueType: "Config", Target: name,
					Detail: fmt.Sprintf("Setting: %s, value(units): %s %s (%.2fMB) - %s\nGoal: Total RAM * 0.25 / max_connections(%d)\n",
						name, s.value, s.units, float32(workMem)/float32(utils.SIZE_MB), issue, maxConnectionsSetting),
					Severity: utils.High,
					Solution: fmt.Sprintf("Update postgresql.conf -  '%s = %s'\n",
						name, utils.PrettyPrint(utils.CleartoMB(int64(target))))})
			}
		case "max_connections":

		default:
			fmt.Printf("ERROR: Internal error - unexpected parameter name '%s' with value: %s, units: %s\n", name, s.value, s.units)
		}
	}
}

func testRange(v int64, lowBound int64, highBound int64) string {
	if v < lowBound {
		return "low"
	}
	if v > highBound {
		return "high"
	}

	return ""
}

func (d *ConfigIssues) GetIssues() []utils.Issue {
	return d.issues
}

func (d *ConfigIssues) GetDurationMS() int64 {
	return d.timing.GetDurationMS()
}
