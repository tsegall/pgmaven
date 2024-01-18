package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"cobber.com/dbinfo"
	"github.com/elliotchance/sshtunnel"
	"golang.org/x/crypto/ssh"

	// _ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq" // Register the driver
)

const (
	dbname     = "testdb"
	host       = "localhost"
	port       = 5432
	password   = "<SETME>"
	schema     = "public"
	tunnelPort = 22
	username   = "tsegall"
)

type Options struct {
	DBName               string
	DBNames              string
	CreateTables         bool
	DuplicateIndex       bool
	ResetIndexData       bool
	Host                 string
	Password             string
	Port                 int
	Query                string
	QueryRows            string
	Schema               string
	SnapShot             bool
	TunnelHost           string
	TunnelPort           int
	TunnelPrivateKeyFile string
	TunnelUsername       string
	Username             string
	Verbose              bool
}

func PrivateKeyFileWithPassphrase(file string, passphrase []byte) ssh.AuthMethod {
	buffer, err := os.ReadFile(file)
	if err != nil {
		return nil
	}

	key, err := ssh.ParsePrivateKeyWithPassphrase(buffer, passphrase)
	if err != nil {
		return nil
	}

	return ssh.PublicKeys(key)
}

func main() {
	var options Options

	flag.BoolVar(&options.CreateTables, "createTables", false, "create tables required for tracking activity over time")
	flag.StringVar(&options.DBNames, "dbnames", "", "file with a list of dbnames to connect to")
	flag.StringVar(&options.DBName, "dbname", dbname, "database name to connect to")
	flag.BoolVar(&options.DuplicateIndex, "duplicateIndex", false, "check for duplicate indexes")
	flag.StringVar(&options.Host, "host", host, "database server host or socket directory (default: 'local socket')")
	flag.StringVar(&options.Password, "password", password, "password for DB")
	flag.IntVar(&options.Port, "port", port, "database server port (default: '5432')")
	flag.StringVar(&options.Query, "query", "", "query (single row) to execute across all DBs provided")
	flag.StringVar(&options.QueryRows, "queryRows", "", "query (multiple rows) to execute across all DBs provided")
	flag.BoolVar(&options.ResetIndexData, "resetIndexData", false, "reset index data")
	flag.StringVar(&options.Schema, "schema", schema, "database schema (default: 'public')")
	flag.BoolVar(&options.SnapShot, "snapShot", false, "snapshot statistics tables")
	flag.StringVar(&options.TunnelHost, "tunnelHost", "", "hostname of tunnel server")
	flag.IntVar(&options.TunnelPort, "tunnelPort", tunnelPort, "port for tunnel server default: '22')")
	flag.StringVar(&options.TunnelPrivateKeyFile, "tunnelPrivateKeyFile", "", "path to private key file")
	flag.StringVar(&options.TunnelUsername, "tunnelUsername", "", "username for tunnel server")
	flag.StringVar(&options.Username, "username", username, "database user name")
	flag.BoolVar(&options.Verbose, "verbose", false, "enable verbose logging")

	flag.Parse()

	var tunnel *sshtunnel.SSHTunnel

	tunnel = nil

	if options.TunnelHost != "" {
		var err error
		// Setup the tunnel, but do not yet start it yet.
		tunnel, err = sshtunnel.NewSSHTunnel(
			// User and host of tunnel server, it will default to port 22
			// if not specified.
			options.TunnelUsername+"@"+options.TunnelHost,

			PrivateKeyFileWithPassphrase(options.TunnelPrivateKeyFile, []byte("Linux is not all bad")),

			// The destination host and port of the actual server.
			options.Host+":"+strconv.Itoa(options.Port),

			// The local port you want to bind the remote port to.
			// Specifying "0" will lead to a random port.
			"0",
		)
		if err != nil {
			log.Fatalf("ERROR: Failed to establish tunnel, error: %v\n", err)
		}
	}

	if tunnel != nil {
		if options.Verbose {
			tunnel.Log = log.New(os.Stdout, "", log.Ldate|log.Lmicroseconds)
		}

		go tunnel.Start()
		time.Sleep(500 * time.Millisecond)
	}

	var dbnames []string
	if options.DBNames != "" {
		content, err := os.ReadFile(options.DBNames)
		if err != nil {
			log.Fatalf("ERROR: Failed to open file, error %v\n", err)
		}
		dbnames = strings.Split(string(content), "\n")
	} else {
		dbnames = []string{options.DBName}
	}

	for _, dbname := range dbnames {

		if strings.Trim(dbname, " ") == "" {
			continue
		}

		var psqlInfo string

		if tunnel != nil {
			psqlInfo = fmt.Sprintf("host=%s port=%d user=%s "+
				"password=%s dbname=%s sslmode=disable",
				"localhost", tunnel.Local.Port, options.Username, options.Password, dbname)
		} else {
			psqlInfo = fmt.Sprintf("host=%s port=%d user=%s "+
				"password=%s dbname=%s sslmode=disable",
				options.Host, options.Port, options.Username, options.Password, dbname)
		}

		if options.Verbose {
			fmt.Printf("Connection String: %s\n", psqlInfo)
		}

		db, err := sql.Open("postgres", psqlInfo)
		if err != nil {
			log.Printf("ERROR: Database: %s, open failed with error: %v\n", dbname, err)
			continue
		}

		err = db.Ping()
		if err != nil {
			log.Printf("ERROR: Database: %s, failed to ping database, error: %v\n", dbname, err)
			continue
		}

		dbinfo.Init(db)

		// If we are processing multiple databases then output the name of the DB we are working on
		if options.DBNames != "" {
			fmt.Printf("Database: %s\n", dbname)
		}

		if options.Query != "" {
			var result string
			err, result = dbinfo.ExecuteQueryRow(options.Query)
			if err != nil {
				log.Printf("ERROR: Database: %s, Query '%s' failed with error: %v\n", dbname, options.Query, err)
				continue
			}
			fmt.Printf("Database: %s, Query '%s', result: %s\n", dbname, options.Query, result)
		}

		if options.QueryRows != "" {
			err = dbinfo.OutputQueryRows(options.QueryRows)
			if err != nil {
				log.Printf("ERROR: Database: %s, Query '%s' failed with error: %v\n", dbname, options.QueryRows, err)
				continue
			}
		}

		if options.DuplicateIndex {
			dbinfo.DuplicateIndexes()
			continue
		}

		if options.ResetIndexData {
			dbinfo.ResetIndexData()
			continue
		}

		if options.CreateTables {
			dbinfo.CreateTables()
			continue
		}

		if options.SnapShot {
			dbinfo.SnapShot()
			continue
		}

		db.Close()
	}
}
