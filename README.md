# polyhouse

Create ClickHouse tables and ingest [polycsv](../polycsv) files into ClickHouse.
Also ingests live data from websockets.

## Schemas
```sh
python schema.py
```

## Ingest
```sh
python ingest.py <dir>
```

## Live ingest
```sh
zig build run
```
