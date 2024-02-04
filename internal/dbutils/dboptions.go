package dbutils

type DBOptions struct {
	DBName               string
	DBNames              string
	Host                 string
	Password             string
	Port                 int
	Schema               string
	TunnelHost           string
	TunnelPort           int
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
	username          = "tsegall"
)
