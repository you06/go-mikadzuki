package config

type Global struct {
	DSN      string `toml:"dsn"`
	Database string `toml:"database"`
	Target   string `toml:"target"`
	Thread   int    `toml:"thread"`
	Action   int    `toml:"action"`
}

func NewGlobal() Global {
	return Global{
		DSN:      "root:@tcp(172.17.0.1:4000)/",
		Database: "mikadzuki",
		Target:   "mysql",
		Thread:   8,
		Action:   20,
	}
}
