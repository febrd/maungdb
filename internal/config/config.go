package config

var (
	DataDir   = "maung_data"
	SystemDir = "_system"

	AllowedExt = []string{".mg", ".maung"}

	DefaultUser = "maung"
	DefaultPass = "maung"
	DefaultRole = "supermaung"

	Roles = map[string]int{
		"supermaung": 0,
		"admin":      1,
		"user":       2,
	}

	SessionFile = "session.maung"
)
