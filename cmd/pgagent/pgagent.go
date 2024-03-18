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
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	flag "github.com/spf13/pflag"

	"pgmaven/internal/commands"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"

	// _ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq" // Register the driver
)

const (
	password     = "<SETME>"
	username     = "tsegall"
	DurationMin  = 60 * 1000 * 1000 * 1000
	DurationHour = 60 * DurationMin
	FrequencyMin = 15 * DurationMin
	FrequencyMax = 24 * DurationHour
)

func main() {
	var (
		optionsDB dbutils.DBOptions
		options   Options
		context   utils.Context
	)

	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err)
	}

	buffer, err := os.ReadFile(filepath.Join(homeDir, ".pgpass"))
	if err == nil {
		fmt.Printf("%s\n", buffer)
	}

	optionsDB.Init()

	flag.BoolVar(&context.Verbose, "verbose", false, "enable verbose logging")

	flag.DurationVar(&options.Frequency, "frequency", DurationHour, "Snapshot frequency")
	flag.BoolVar(&options.Version, "version", false, "print version number")

	flag.Parse()

	if options.Frequency < FrequencyMin || options.Frequency > FrequencyMax {
		log.Fatalf("ERROR: Frequency should be between %d and %d\n", FrequencyMin, FrequencyMax)
	}

	if options.Version {
		fmt.Println(utils.GetVersionString())
		return
	}

	ds := dbutils.NewDataSource(optionsDB)

	var dbnames []string
	if optionsDB.DBNames != "" {
		if optionsDB.DBName != "" {
			log.Fatalf("ERROR: Cannot specify both dbname and dbnames options\n")
		}
		content, err := os.ReadFile(optionsDB.DBNames)
		if err != nil {
			log.Fatalf("ERROR: Failed to open file, error %v\n", err)
		}
		dbnames = strings.Split(string(content), "\n")
	} else {
		dbnames = []string{optionsDB.DBName}
	}

	connectionIntact := true
	for {
		dbs := len(dbnames)
		failures := 0

		for _, dbName := range dbnames {
			if strings.Trim(dbName, " ") == "" {
				continue
			}

			ds.SetDBName(dbName)
			psqlInfo := ds.GetDataSourceString()

			if context.Verbose {
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
				// Don't bother reporting the issue if we know the connection is broken, it will be reported the first time
				if connectionIntact {
					log.Printf("ERROR: Database: %s, failed to ping database, error: %v\n", dbName, err)
				}
				failures++
				continue
			}

			// If we are processing multiple databases then output the name of the DB we are working on
			if optionsDB.DBNames != "" {
				fmt.Printf("Database: %s\n", dbName)
			}

			// Snapshot the Statistics tables
			snapshot := commands.Snapshot{}
			snapshot.Init(context, ds)
			snapshot.Execute()

			db.Close()
		}

		// If we failed to connect to any DB then mark the connection as broken, and sleep for a shorter period before retrying
		if failures == dbs {
			time.Sleep(5 * DurationMin)
			connectionIntact = false
		} else {
			if !connectionIntact {
				log.Printf("ERROR: Database: %s, Connection re-established\n", dbnames[0])
				connectionIntact = true
			}
			time.Sleep(options.Frequency)
		}
	}
}
