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

	"pgmaven/internal/commands"
	"pgmaven/internal/dbutils"
	"pgmaven/internal/plugins"
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
	var options utils.Options

	flag.StringVar(&options.DBNames, "dbnames", "", "file with a list of dbnames to connect to")
	flag.StringVar(&options.DBName, "dbname", dbname, "database name to connect to")
	flag.StringVar(&options.Host, "host", host, "database server host or socket directory (default: 'local socket')")
	flag.StringVar(&options.Password, "password", password, "password for DB")
	flag.IntVar(&options.Port, "port", port, "database server port (default: '5432')")
	flag.StringVar(&options.Schema, "schema", schema, "database schema (default: 'public')")
	flag.StringVar(&options.TunnelHost, "tunnelHost", "", "hostname of tunnel server")
	flag.IntVar(&options.TunnelPort, "tunnelPort", tunnelPort, "port for tunnel server default: '22')")
	flag.StringVar(&options.TunnelPrivateKeyFile, "tunnelPrivateKeyFile", "", "path to private key file")
	flag.StringVar(&options.TunnelUsername, "tunnelUsername", "", "username for tunnel server")
	flag.StringVar(&options.Username, "username", username, "database user name")
	flag.BoolVar(&options.Verbose, "verbose", false, "enable verbose logging")
	flag.BoolVar(&options.Version, "version", false, "print version number")

	flag.StringVar(&options.Command, "command", "", "execute the command specified (--command Help for options)")
	flag.StringVar(&options.Detect, "detect", "", "execute the issue detection specified (--detect Help for options)")

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

		if options.Verbose {
			tunnel.Log = log.New(os.Stdout, "", log.Ldate|log.Lmicroseconds)
		}

		go tunnel.Start()
		time.Sleep(500 * time.Millisecond)
	}

	var dbnames []string
	if options.DBNames != "" {
		if options.DBName != "" {
			log.Fatalf("ERROR: Cannot specify both dbname and dbnames options\n")
		}
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

		dbutils.Init(db, options, dbname, options.Schema)

		// If we are processing multiple databases then output the name of the DB we are working on
		if options.DBNames != "" {
			fmt.Printf("Database: %s\n", dbname)
		}

		if options.Detect != "" {
			detectOptions := strings.Split(options.Detect, ":")
			detector, err := plugins.NewDetector(detectOptions[0])
			if err != nil {
				log.Println("ERROR: Failed to locate detector\n", err)
				continue
			}
			detector.Execute(detectOptions[1:]...)
			for _, issue := range detector.GetIssues() {
				issue.Dump()
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
			command.Execute(commandOptions[1:]...)
			continue
		}

		db.Close()
	}
}
