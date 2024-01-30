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
	"strconv"
	"strings"
	"time"

	"pgmaven/internal/dbutils"
	"pgmaven/internal/utils"

	"github.com/elliotchance/sshtunnel"
	"golang.org/x/crypto/ssh"

	// _ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq" // Register the driver
)

const (
	dbname     = ""
	host       = "localhost"
	port       = 5432
	password   = "<SETME>"
	schema     = "public"
	tunnelPort = 22
	username   = "tsegall"
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
	var optionsDB dbutils.DBOptions
	var options Options

	flag.StringVar(&optionsDB.DBNames, "dbnames", "", "file with a list of dbnames to connect to")
	flag.StringVar(&optionsDB.DBName, "dbname", dbname, "database name to connect to")
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

	flag.Parse()

	if options.Version {
		fmt.Println(utils.GetVersionString())
		return
	}
	var tunnel *sshtunnel.SSHTunnel

	tunnel = nil

	if optionsDB.TunnelHost != "" {
		var err error
		// Setup the tunnel, but do not yet start it yet.
		tunnel, err = sshtunnel.NewSSHTunnel(
			// User and host of tunnel server, it will default to port 22
			// if not specified.
			optionsDB.TunnelUsername+"@"+optionsDB.TunnelHost,

			PrivateKeyFileWithPassphrase(optionsDB.TunnelPrivateKeyFile, []byte("Linux is not all bad")),

			// The destination host and port of the actual server.
			optionsDB.Host+":"+strconv.Itoa(optionsDB.Port),

			// The local port you want to bind the remote port to.
			// Specifying "0" will lead to a random port.
			"0",
		)
		if err != nil {
			log.Fatalf("ERROR: Failed to establish tunnel, error: %v\n", err)
		}

		if options.Verbose {
			tunnel.Log = log.New(os.Stdout, "", log.Ldate|log.Lmicroseconds)
		}

		go tunnel.Start()
		time.Sleep(500 * time.Millisecond)
	}

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

	for _, dbname := range dbnames {

		if strings.Trim(dbname, " ") == "" {
			continue
		}

		var psqlInfo string

		if tunnel != nil {
			psqlInfo = fmt.Sprintf("host=%s port=%d user=%s "+
				"password=%s dbname=%s sslmode=disable",
				"localhost", tunnel.Local.Port, optionsDB.Username, optionsDB.Password, dbname)
		} else {
			psqlInfo = fmt.Sprintf("host=%s port=%d user=%s "+
				"password=%s dbname=%s sslmode=disable",
				optionsDB.Host, optionsDB.Port, optionsDB.Username, optionsDB.Password, dbname)
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

		dbutils.Init(db, optionsDB, dbname)

		// If we are processing multiple databases then output the name of the DB we are working on
		if optionsDB.DBNames != "" {
			fmt.Printf("Database: %s\n", dbname)
		}

		db.Close()
	}
}
