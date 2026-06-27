//go:build mage

// Mage build file for nyro.
// Powered by nava (https://github.com/nirantaraai/nava).
//
// Usage:
//
//	go install github.com/magefile/mage@latest
//	mage -l          # list targets
//	mage test        # run tests
//	mage lint        # run golangci-lint
package main

import (
	"fmt"
	"os"

	gomagex "github.com/nirantaraai/nava/mage/golang"
	goreleasermagex "github.com/nirantaraai/nava/mage/goreleaser"
)

// init loads all YAML configs once before any target runs.
func init() {
	_ = gomagex.LoadConfig("go.yaml")
	_ = goreleasermagex.LoadConfig("goreleaser.yaml")
}

// Test runs the unit test suite (config: go.yaml → test).
func Test() error { return gomagex.Test() }

// Lint runs golangci-lint (config: go.yaml → lint).
func Lint() error { return gomagex.Lint() }

// Vet runs go vet (config: go.yaml → vet).
func Vet() error { return gomagex.Vet() }

// Setup downloads Go modules (config: go.yaml → setup).
func Setup() error { return gomagex.Setup() }

// Race runs tests with race detection (config: go.yaml → race).
func Race() error { return gomagex.Race() }

// Coverage runs tests with coverage profiling (config: go.yaml → coverage).
func Coverage() error { return gomagex.Coverage() }

// Bench runs benchmarks (config: go.yaml → bench).
func Bench() error { return gomagex.Bench() }

// Govulncheck runs govulncheck for dependency vulnerability scanning.
func Govulncheck() error { return gomagex.Govulncheck() }

// Clean removes build artefacts.
func Clean() error {
	fmt.Println("cleaning dist/ coverage.out")
	_ = os.Remove("coverage.out")
	return os.RemoveAll("dist")
}

// Release creates a GitHub release via goreleaser (config: goreleaser.yaml).
func Release() error { return goreleasermagex.Release() }

// Snapshot creates a local snapshot build without publishing (config: goreleaser.yaml).
func Snapshot() error { return goreleasermagex.Snapshot() }
