# harlequin-adapter-template

This repo provides the Harlequin adapter for clickhouse.
It is based on the [harlequin-adapter-template](https://github.com/tconbeer/harlequin-adapter-template)

See [Harlequin](https://harlequin.sh) docs for more database adapter.

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
