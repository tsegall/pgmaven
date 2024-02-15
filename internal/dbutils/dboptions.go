package dbutils

import (
	flag "github.com/spf13/pflag"
)

type DBOptions struct {
	DBName               string
	DBNames              string
	Host                 string
	Password             string
	Port                 int
	Schema               string
	TunnelHost           string
	TunnelPort           int
	TunnelPassphrase     string
	TunnelPrivateKeyFile string
	TunnelUsername       string
	Username             string
}

const (
	DefaultHost       = "localhost"
	DefaultPort       = 5432
	password          = "<SETME>"
	DefaultSchema     = "public"
	DefaultTunnelPort = 22
)

func (o *DBOptions) Init() {
	flag.StringVar(&o.DBNames, "dbnames", "", "file with a list of dbnames to connect to")
	flag.StringVar(&o.DBName, "dbname", "", "database name to connect to")
	flag.StringVar(&o.Host, "host", DefaultHost, "database server host or socket directory (default: 'local socket')")
	flag.StringVar(&o.Password, "password", "", "password for DB")
	flag.IntVar(&o.Port, "port", DefaultPort, "database server port (default: '5432')")
	flag.StringVar(&o.Schema, "schema", DefaultSchema, "database schema (default: 'public')")
	flag.StringVar(&o.TunnelHost, "tunnelHost", "", "hostname of tunnel server")
	flag.IntVar(&o.TunnelPort, "tunnelPort", DefaultTunnelPort, "port for tunnel server default: '22')")
	flag.StringVar(&o.TunnelPassphrase, "tunnelPassphrase", "", "passphrase for private key file")
	flag.StringVar(&o.TunnelPrivateKeyFile, "tunnelPrivateKeyFile", "", "path to private key file")
	flag.StringVar(&o.TunnelUsername, "tunnelUsername", "", "username for tunnel server")
	flag.StringVar(&o.Username, "username", "", "database user name")
}
