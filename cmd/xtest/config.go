package main

type Config struct {
	// Name of the config to use
	Name              string `json:"name"`
	LogLevel          string `json:"logLevel"`
	DebugPort         int    `json:"debugPort"`
	MemcachedAddr     string `json:"memcachedAddr"`
	SourceKVAdminPort int    `json:"sourceKVAdminPort"`
	XdcrRestPort      int    `json:"xdcrRestPort"`

	ConflictLogPertest *ConflictLogLoadTest `json:"conflictLogLoadTest"`
	CBAuthTest         *CBAuthTest          `json:"cbauthTest"`
	ThrottlerTest      *ThrottlerTest       `json:"throttlerTest"`
	GomemcachedTest    *GomemcachedTest     `json:"gomemcachedTest"`

	ClientCertFile string `json:"clientCertFile"`
	ClientKeyFile  string `json:"clientKeyFile"`
	ClusterCAFile  string `json:"clusterCAFile"`

	EncryptionLevelStrict       bool `json:"encryptionLevelStrict"`
	BypassSanInCertificateCheck bool `json:"bypassSanInCertificateCheck"`
}
