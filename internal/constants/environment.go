package constants

// Environment mirrors the Environment enum used across the platform.
type Environment string

const (
	EnvironmentLocal Environment = "local"
	EnvironmentDev   Environment = "dev"
	EnvironmentProd  Environment = "prod"
)

// MakeEnvironment resolves the ENV variable, defaulting to local.
func MakeEnvironment(env string) Environment {
	switch env {
	case "dev":
		return EnvironmentDev
	case "prod":
		return EnvironmentProd
	default:
		return EnvironmentLocal
	}
}
