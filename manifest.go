package main

type Manifest struct {
	Tasks []ManifestTask `toml:"task"`
}

type ManifestTask struct {
	Name     string `toml:"name"`
	Script   string `toml:"script"`
	Start    bool   `toml:"start"`
	Parallel *int   `toml:"parallel"`
}
