# go-stats v1

This branch contains the v1 of go-stats. The v1 relies on seed files to recursively download the dependencies of each of the repositories from the seed.

## Build & Run

- `task init-seed`: init the seed-files from multiple sources. This results in a list of Git repositories
- `task convert-seed`: convert the Git repositories from the seed files to a list of actual Go modules
  - each repository from the seed is cloned locally in order to find and parse its `go.mod` file(s)
- `docker compose up -d`: start a Neo4j instance
  - the Neo4j browser is accessible on http://localhost:7474/ (no username/password required)
- `task process`: load the modules to Neo4j
