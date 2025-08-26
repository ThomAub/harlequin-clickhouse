# harlequin-clickhouse

A Harlequin adapter for ClickHouse databases.

## Installation

You must install the `harlequin-clickhouse` package into the same environment as harlequin. The best and easiest way to do this is to use uv to install Harlequin with the additional package:

```bash
uv tool install harlequin --with harlequin-clickhouse
```

## Usage and Configuration

Run Harlequin with the `-a clickhouse` option and pass a ClickHouse DSN as an argument:

```bash
harlequin -a clickhouse "clickhouse://default:@localhost:9000/default"
```

You can also pass all or parts of the connection string as separate options. The following is equivalent to the above DSN:

```bash
harlequin -a clickhouse --host localhost --port 9000 --database default --user default
```

Many more options are available; to see the full list, run:

```bash
harlequin -a clickhouse --help
```

## Setup ClickHouse

This is from the [ClickHouse single node with Keeper](https://github.com/ClickHouse/examples/blob/main/docker-compose-recipes/recipes/ch-1S_1K/README.md) in the [ClickHouse Examples](https://github.com/ClickHouse/examples) repo

Single node ClickHouse instance leveraging 1 ClickHouse Keeper

By default the version of ClickHouse used will be `latest`, and ClickHouse Keeper
will be `latest-alpine`. You can specify specific versions by setting environment
variables before running `docker compose up`.

```bash
export CHVER=23.4
export CHKVER=23.4-alpine
docker compose up
```
