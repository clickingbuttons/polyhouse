# polyhouse

Currently supports stocks ticker details and trades with materialized views for aggregates.

```sh
polyhouse creates schema for Polygon data and optionally ingests from public APIs

Usage:
  polyhouse [flags]
  polyhouse [command]

Available Commands:
  completion  Generate the autocompletion script for the specified shell
  help        Help about any command
  ingest      Ingests data from Polygon.io into Clickhouse
  schema      Creates Clickhouse schemas for Polygon data

Flags:
      --address string       clickhouse address (default "127.0.0.1:9000")
      --cluster string       clickhouse cluster to make tables on
      --database string      name of database (default "us_equities")
  -h, --help                 help for polyhouse
      --max-idle-conns int   clickhouse max open connections (default 5)
      --max-open-conns int   clickhouse max open connections (default 90)
      --password string      for clickhouse auth
      --templates string     glob for schema templates (default "./templates/*")
      --username string      for clickhouse auth (default "default")
      --verbose              log moar

Use "polyhouse [command] --help" for more information about a command.
```
