package dbutils

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/elliotchance/sshtunnel"
	"golang.org/x/crypto/ssh"
)

type DataSource struct {
	tunnel   *sshtunnel.SSHTunnel
	options  DBOptions
	dbName   string
	database *sql.DB
}

func PrivateKeyFileWithPassphrase(file string, passphrase string) ssh.AuthMethod {
	buffer, err := os.ReadFile(file)
	if err != nil {
		return nil
	}

	key, err := ssh.ParsePrivateKeyWithPassphrase(buffer, []byte(passphrase))
	if err != nil {
		return nil
	}

	return ssh.PublicKeys(key)
}

func NewDataSource(o DBOptions) *DataSource {
	ret := &DataSource{}
	ret.options = o
	if o.TunnelHost != "" {
		if o.TunnelPassphrase == "" {
			log.Fatalf("ERROR: Required passphrase for Key file missing\n")
		}

		var err error
		// Setup the tunnel, but do not yet start it yet.
		ret.tunnel, err = sshtunnel.NewSSHTunnel(
			// User and host of tunnel server, it will default to port 22
			// if not specified.
			o.TunnelUsername+"@"+o.TunnelHost,

			PrivateKeyFileWithPassphrase(o.TunnelPrivateKeyFile, o.TunnelPassphrase),

			// The destination host and port of the actual server.
			o.Host+":"+strconv.Itoa(o.Port),

			// The local port you want to bind the remote port to.
			// Specifying "0" will lead to a random port.
			"0",
		)
		if err != nil {
			log.Fatalf("ERROR: Failed to establish tunnel, error: %v\n", err)
		}

		go ret.tunnel.Start()
		time.Sleep(500 * time.Millisecond)
	}

	return ret
}

func (ds *DataSource) SetDBName(dbName string) {
	ds.dbName = dbName
}

func (ds *DataSource) GetDBName() string {
	return ds.dbName
}

func (ds *DataSource) SetDatabase(db *sql.DB) {
	ds.database = db
}

func (ds *DataSource) GetDatabase() *sql.DB {
	return ds.database
}

func (ds *DataSource) GetSchema() string {
	return ds.options.Schema
}

func (ds *DataSource) GetDataSourceString() string {
	password := ds.options.Password
	if password == "" {
		password = "''"
	}
	if ds.tunnel != nil {
		return fmt.Sprintf("host=%s port=%d user=%s "+
			"password=%s dbname=%s sslmode=disable",
			"localhost", ds.tunnel.Local.Port, ds.options.Username, password, ds.dbName)
	}

	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		ds.options.Host, ds.options.Port, ds.options.Username, password, ds.dbName)
}

func (ds *DataSource) ExecuteQueryRows(query string, queryArgs []any, processor func(int, []*sql.ColumnType, []interface{}, any), processorArg any) error {
	var rows *sql.Rows
	var err error

	rows, err = ds.database.Query(query, queryArgs...)

	if err != nil {
		fmt.Printf("ERROR: Database: %s, Failed to query database, error: %v\n", ds.GetDBName(), err)
		return err
	}
	defer rows.Close()

	columnsTypes, err := rows.ColumnTypes()
	if err != nil {
		fmt.Printf("ERROR: Database: %s, Failed to get Column Types, error: %v\n", ds.GetDBName(), err)
		return err
	}

	if columnsTypes == nil {
		return nil
	}

	vals := make([]interface{}, len(columnsTypes))
	for i := 0; i < len(columnsTypes); i++ {
		vals[i] = new(interface{})
	}

	rowNumber := 1
	// Process each row
	for rows.Next() {
		err = rows.Scan(vals...)
		if err != nil {
			fmt.Println(err)
			continue
		}
		processor(rowNumber, columnsTypes, vals, processorArg)
		rowNumber++
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	return nil
}

func (ds *DataSource) ExecuteQueryRow(query string, queryArgs []any) (any, error) {
	row := ds.database.QueryRow(query, queryArgs...)

	var result any
	err := row.Scan(&result)
	if err != nil {
		log.Printf("ERROR: Database: %s, Failed to get row, error: %v\n", ds.GetDBName(), err)
		return "", err
	}

	return result, nil
}

func (ds *DataSource) Exec(statement string, statementArgs []any) (sql.Result, error) {
	result, err := ds.database.Exec(statement, statementArgs...)

	if err != nil {
		log.Printf("ERROR: Database: %s, Failed to get row, error: %v\n", ds.GetDBName(), err)
		return nil, err
	}

	return result, nil
}

func (ds *DataSource) TableList(minRows int) ([]string, error) {
	var rows *sql.Rows
	var err error

	if minRows == -1 {
		rows, err = ds.GetDatabase().Query(`SELECT table_name FROM information_schema.tables where table_schema = $1 and table_type = 'BASE TABLE' and table_name not ilike 'PGMAVEN_%'`, ds.options.Schema)
	} else {
		rows, err = ds.GetDatabase().Query(`
			SELECT table_name FROM information_schema.tables, pg_stat_user_tables
			where table_name = relname
		  	  and table_schema = $1
		  	  and table_type = 'BASE TABLE'
		  	  and table_name not ilike 'PGMAVEN_%'
		  	  and n_live_tup > $2`, ds.options.Schema, minRows)
	}
	if err != nil {
		fmt.Printf("ERROR: Failed to query database, error: %v\n", err)
		return nil, err
	}
	defer rows.Close()

	ret := make([]string, 0)
	var table_name string

	for rows.Next() {
		err := rows.Scan(&table_name)
		if err != nil {
			log.Printf("ERROR: Database: %s, Failed to get row, error: %v\n", ds.GetDBName(), err)
			return nil, err
		}
		ret = append(ret, table_name)
	}
	return ret, nil
}

// IndexDefinition returns the DDL for the named index.
func (ds *DataSource) IndexDefinition(indexName string) string {
	query := fmt.Sprintf(`SELECT pg_get_indexdef('%s'::regclass);`, indexName)
	ret, err := ds.ExecuteQueryRow(query, nil)
	if err != nil {
		log.Printf("ERROR: Database: %s, IndexDefinition failed with error: %v\n", ds.GetDBName(), err)
		return ""
	}

	return ret.(string)
}

// Get closest record to the time provided based on the supplied table
func (d *DataSource) GetClosest(table string, t time.Time) any {
	query := `with
	date_options as (
	select
		distinct(insert_dt) as insert_dt
	from
		%s),
	closest as (
	select
		insert_dt,
		abs(extract(epoch from insert_dt - $1 AT TIME ZONE 'UTC')) as diff
	from
		date_options
	order by
		diff asc
	limit 1)
	select
		insert_dt
	from
		closest
	`
	closest, _ := d.ExecuteQueryRow(fmt.Sprintf(query, table), []any{t})

	return closest
}
