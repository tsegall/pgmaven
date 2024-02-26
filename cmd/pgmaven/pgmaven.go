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
	"strings"

	flag "github.com/spf13/pflag"

	"pgmaven/internal/commands"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/plugins"
	"pgmaven/internal/utils"

	// _ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq" // Register the driver
)

const (
	username     = "tsegall"
	DurationWeek = 7 * 24 * 60 * 60 * 1000 * 1000 * 1000
)

func main() {
	var (
		optionsDB dbutils.DBOptions
		options   Options
		context   utils.Context
	)

	optionsDB.Init()

	flag.DurationVar(&context.Duration, "duration", DurationWeek, "Duration of analysis - default week")
	flag.DurationVar(&context.DurationOffset, "durationOffset", 0, "Duration offset (from now) - 0")
	flag.BoolVar(&context.Verbose, "verbose", false, "enable verbose logging")

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
		if optionsDB.DBName == "" {
			optionsDB.DBName = "''"
		}
		dbNames = []string{optionsDB.DBName}
	}

	for _, dbName := range dbNames {
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
			log.Printf("ERROR: Database: %s, failed to ping database, error: %v\n", dbName, err)
			continue
		}

		// If we are processing multiple databases then output the name of the DB we are working on
		if optionsDB.DBNames != "" {
			fmt.Printf("Database: %s\n", dbName)
		}

		if options.Detect != "" {
			detectOptions := strings.Split(options.Detect, ":")
			detector, err := plugins.NewDetector(detectOptions[0])
			if err != nil {
				log.Println("ERROR: Failed to locate detector\n", err)
				continue
			}
			detector.Init(context, ds)
			detector.Execute(detectOptions[1:]...)
			for _, issue := range detector.GetIssues() {
				issue.Dump()
			}
			if context.Verbose {
				fmt.Printf("Execution Time: %dms\n", detector.GetDurationMS())
			}
			continue
		}

		if options.Command != "" {
			commandOptions := strings.Split(options.Command, ":")
			command, err := commands.NewCommand(commandOptions[0])
			if err != nil {
				log.Println("ERROR: Failed to locate command\n", err)
				continue
			}
			command.Init(context, ds)
			command.Execute(commandOptions[1:]...)
			continue
		}

		db.Close()
	}
}
