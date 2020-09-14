package config

import (
	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

type Config struct {
	Global Global `toml:"global"`
	Graph  Graph  `toml:"graph"`
	Depend Depend `toml:"depend"`
}

func NewConfig() Config {
	return Config{
		Global: NewGlobal(),
		Graph:  NewGraph(),
		Depend: NewDepend(),
	}
}

// Load config from file
func (c *Config) Load(file string) error {
	_, err := toml.DecodeFile(file, c)
	return errors.Trace(err)
}
