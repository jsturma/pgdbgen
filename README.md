# pgdbgen

`pgdbgen` is a small PostgreSQL test database generator/loader that uses [`go-faker/faker`](https://github.com/go-faker/faker) to create “realistic-ish” demo data.

It will:

- create the target database if it doesn’t exist
- create a small schema (`payments`, `buying_stats`, `products`, `accounts`) if missing
- insert a configurable number of rows using a configurable number of worker goroutines

## What it creates

Tables (created if missing):

- **`payments`**: `p_md5`, `p_amount`, `p_epoch`
- **`buying_stats`**: user/product UUIDs, quantity, total amount, epoch
- **`products`**: UUID, name, authors, price
- **`accounts`**: UUID, username/email/password, created epoch, last login epoch

## Install / build

This repository currently **does not include a `go.mod`**. The easiest way to build locally is to create one in-place:

```bash
cd /path/to/pgdbgen
go mod init github.com/jsturma/pgdbgen
go mod tidy
go build -o pgdbgen ./pgdbgen.go
```

### Prebuilt binaries in `bin/`

`bin/pgdbgen` and `bin/tools/dbload` are **Linux ELF** binaries (x86_64). They won’t run on macOS/Windows; build from source on those platforms.

## Usage

Run with flags only:

```bash
./pgdbgen \
  -host localhost \
  -port 5432 \
  -user postgres \
  -password postgres \
  -dbname mytestdb \
  -numWorkers 5 \
  -dbRecords2Process 10000
```

Run with YAML config (plus any flag overrides you want):

```bash
./pgdbgen -config ./bin/example.yaml -dbname mytestdb -dbRecords2Process 5000
```

### CLI flags

All flags have defaults; `-config` is optional.

- **`-host`**: Postgres host (default `localhost`)
- **`-port`**: Postgres port (default `5432`)
- **`-user`**: admin user used to create the DB and connect (default `postgres`)
- **`-password`**: admin password (default `postgres`)
- **`-dbname`**: database to create/populate (default `mytestdb`)
- **`-config`**: path to YAML config file (default empty)
- **`-numWorkers`**: number of concurrent workers inserting rows (default `3`)
- **`-dbRecords2Process`**: number of “records” to process (default `100`)
- **`-pcentOutput`**: progress output every X% (default `10`)
- **`-minDays`**: minimum “account created” offset in seconds (default `259200` = 3 days)
- **`-maxDays`**: maximum “account created” offset in seconds (default `31536000` = 1 year)
- **`-delayLastLogin`**: random last-login delay in seconds (default `500`)
- **`-runOnlyFaker`**: verbose/debug-ish mode in code (default `false`)

## YAML configuration

See `bin/example.yaml` for a starter config. Keys map directly to CLI flags:

```yaml
host: example.com
port: 5432
user: postgres
password: postgres
dbname: mytestdb
runOnlyFaker: false
numWorkers: 5
dbRecords2Process: 100
pcentOutput: 5
minDays: 259200
maxDays: 31536000
delayLastLogin: 500
```

Notes:

- Any CLI flag you pass will override YAML values (YAML is applied first, then flags win by virtue of being explicitly provided at runtime).
- `pcentOutput` affects how frequently progress is logged; it’s converted internally into a “records per log line” count.

## Continuous loader helpers

There are two “looping loader” helpers that repeatedly run `pgdbgen` against any `*.yaml` / `*.yml` files in the current directory, choosing a semi-random record count and sleeping between runs:

- **Bash**: `bin/tools/bash/loadpgdb.bash`
- **Go** (older / kept in `obselete/`): `obselete/dbload.go`

Example (bash helper):

```bash
cd /path/with/configs
cp /path/to/pgdbgen/bin/example.yaml ./local.yaml
/path/to/pgdbgen/pgdbgen -config ./local.yaml -dbname mytestdb -dbRecords2Process 1000
```

Or (continuous loop):

```bash
cd /path/with/configs
/path/to/pgdbgen/bin/tools/bash/loadpgdb.bash mytestdb
```

## Requirements

- Go toolchain (to build from source)
- PostgreSQL reachable with credentials that can create databases (it connects to `postgres` DB to create the target DB)

## License

MIT (see `LICENSE`).
