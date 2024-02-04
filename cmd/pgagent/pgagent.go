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
	"time"

	"pgmaven/internal/commands"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"

	"golang.org/x/crypto/ssh"

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
	var (
		optionsDB dbutils.DBOptions
		options   Options
		context   utils.Context
	)

	flag.StringVar(&optionsDB.DBNames, "dbnames", "", "file with a list of dbnames to connect to")
	flag.StringVar(&optionsDB.DBName, "dbname", "", "database name to connect to")
	flag.StringVar(&optionsDB.Host, "host", dbutils.DefaultHost, "database server host or socket directory (default: 'local socket')")
	flag.StringVar(&optionsDB.Password, "password", password, "password for DB")
	flag.IntVar(&optionsDB.Port, "port", dbutils.DefaultPort, "database server port (default: '5432')")
	flag.StringVar(&optionsDB.Schema, "schema", dbutils.DefaultSchema, "database schema (default: 'public')")
	flag.StringVar(&optionsDB.TunnelHost, "tunnelHost", "", "hostname of tunnel server")
	flag.IntVar(&optionsDB.TunnelPort, "tunnelPort", dbutils.DefaultTunnelPort, "port for tunnel server default: '22')")
	flag.StringVar(&optionsDB.TunnelPrivateKeyFile, "tunnelPrivateKeyFile", "", "path to private key file")
	flag.StringVar(&optionsDB.TunnelUsername, "tunnelUsername", "", "username for tunnel server")
	flag.StringVar(&optionsDB.Username, "username", username, "database user name")

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

	for {
		for _, dbName := range dbnames {

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

			// Snapshot the Statistics tables
			snapshot := commands.Snapshot{}
			snapshot.Init(context, ds)
			snapshot.Execute()

			db.Close()
		}

		time.Sleep(options.Frequency)
	}
}
