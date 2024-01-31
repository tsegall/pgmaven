/*
 * Copyright 2024 Tim Segall
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"pgmaven/internal/commands"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/plugins"
	"pgmaven/internal/utils"

	// _ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq" // Register the driver
)

const (
	host       = "localhost"
	port       = 5432
	password   = "<SETME>"
	schema     = "public"
	tunnelPort = 22
	username   = "tsegall"
)

func main() {
	var optionsDB dbutils.DBOptions
	var options Options

	flag.StringVar(&optionsDB.DBNames, "dbnames", "", "file with a list of dbnames to connect to")
	flag.StringVar(&optionsDB.DBName, "dbname", "", "database name to connect to")
	flag.StringVar(&optionsDB.Host, "host", host, "database server host or socket directory (default: 'local socket')")
	flag.StringVar(&optionsDB.Password, "password", password, "password for DB")
	flag.IntVar(&optionsDB.Port, "port", port, "database server port (default: '5432')")
	flag.StringVar(&optionsDB.Schema, "schema", schema, "database schema (default: 'public')")
	flag.StringVar(&optionsDB.TunnelHost, "tunnelHost", "", "hostname of tunnel server")
	flag.IntVar(&optionsDB.TunnelPort, "tunnelPort", tunnelPort, "port for tunnel server default: '22')")
	flag.StringVar(&optionsDB.TunnelPrivateKeyFile, "tunnelPrivateKeyFile", "", "path to private key file")
	flag.StringVar(&optionsDB.TunnelUsername, "tunnelUsername", "", "username for tunnel server")
	flag.StringVar(&optionsDB.Username, "username", username, "database user name")
	flag.BoolVar(&options.Verbose, "verbose", false, "enable verbose logging")
	flag.BoolVar(&options.Version, "version", false, "print version number")

	flag.StringVar(&options.Command, "command", "", "execute the command specified (--command Help for options)")
	flag.StringVar(&options.Detect, "detect", "", "execute the issue detection specified (--detect Help for options)")

	flag.Parse()

	if options.Version {
		fmt.Println(utils.GetVersionString())
		return
	}

	ds := dbutils.NewDataSource(optionsDB)

	var dbNames []string
	if optionsDB.DBNames != "" {
		if optionsDB.DBName != "" {
			log.Fatalf("ERROR: Cannot specify both dbname and dbnames options\n")
		}
		content, err := os.ReadFile(optionsDB.DBNames)
		if err != nil {
			log.Fatalf("ERROR: Failed to open file, error %v\n", err)
		}
		dbNames = strings.Split(string(content), "\n")
	} else {
		dbNames = []string{optionsDB.DBName}
	}

	for _, dbName := range dbNames {
		if strings.Trim(dbName, " ") == "" {
			continue
		}

		ds.SetDBName(dbName)
		psqlInfo := ds.GetDataSourceString()

		if options.Verbose {
			fmt.Printf("Connection String: %s\n", psqlInfo)
		}

		db, err := sql.Open("postgres", psqlInfo)
		if err != nil {
			log.Printf("ERROR: Database: %s, open failed with error: %v\n", dbName, err)
			continue
		}
		ds.SetDatabase(db)

		err = db.Ping()
		if err != nil {
			log.Printf("ERROR: Database: %s, failed to ping database, error: %v\n", dbName, err)
			continue
		}

		// If we are processing multiple databases then output the name of the DB we are working on
		if optionsDB.DBNames != "" {
			fmt.Printf("Database: %s\n", dbName)
		}

		if options.Detect != "" {
			detectOptions := strings.Split(options.Detect, ":")
			detector, err := plugins.NewDetector(ds, detectOptions[0])
			if err != nil {
				log.Println("ERROR: Failed to locate detector\n", err)
				continue
			}
			detector.Execute(detectOptions[1:]...)
			if options.Verbose {
				fmt.Printf("Execution Time: %dms\n", detector.GetDurationMS())
			}
			for _, issue := range detector.GetIssues() {
				issue.Dump()
			}
			continue
		}

		if options.Command != "" {
			commandOptions := strings.Split(options.Command, ":")
			command, err := commands.NewCommand(ds, commandOptions[0])
			if err != nil {
				log.Println("ERROR: Failed to locate command\n", err)
				continue
			}
			command.Execute(commandOptions[1:]...)
			continue
		}

		db.Close()
	}
}
