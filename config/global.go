package config

type Global struct {
	DependRatio float64 `toml:"depend-ratio"`
}

func NewGlobal() Global {
	return Global{
		DependRatio: 0.2,
	}
}
