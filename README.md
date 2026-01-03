# go-stats

This branch contains the latest version of go-stats. The latest version relies on the [Go modules proxy](https://proxy.golang.org).

The full Go proxy index from years 2019 to 2025 is cached in [data/goproxy-modules](./data/goproxy-modules/).

## Build & Run

- `task init-goproxy-seed`: extract and concatenate the cached Go proxy seed files to process them all at once
- `docker compose up -d`: start a Neo4j instance
  - the Neo4j browser is accessible on http://localhost:7474/ (connection url: `neo4j://localhost:7687` ; no username/password required)
- `task process`: load the modules to Neo4j
