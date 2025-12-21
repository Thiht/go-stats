package semver

import (
	"fmt"
	"strings"
)

type SemanticVersion struct {
	Major string
	Minor string
	Patch string
	Label string
}

func Parse(version string) (semver SemanticVersion, err error) {
	version = strings.TrimPrefix(version, "v")

	version, label, ok := strings.Cut(version, "-")
	if !ok {
		version, label, _ = strings.Cut(version, "+")
	}

	tokens := strings.Split(version, ".")
	if len(tokens) != 3 {
		return SemanticVersion{}, fmt.Errorf("invalid version format: %s", version)
	}

	return SemanticVersion{Major: tokens[0], Minor: tokens[1], Patch: tokens[2], Label: label}, nil
}
