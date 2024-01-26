package utils

type Options struct {
	CreateTables         bool
	DBName               string
	DBNames              string
	Detect               string
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
	Version              bool
}
