from os import path, scandir, DirEntry
import sys
import xml.etree.ElementTree as ET
from schema import database, query
from concurrent.futures import ThreadPoolExecutor
import argparse

suffix = '.csv.gz'

def already_ingested(table: str, f: DirEntry):
    partition = path.basename(f.name).replace(suffix, '').replace('-', '')
    existing = query(f"select count() from system.parts where table='{table}' and partition='{partition}'")
    return existing != '0'

def ingest_trades(f: DirEntry):
    if already_ingested('trades', f):
        print(f"{f.name} (skip)")
        return
    print(f"{f.name} {f.stat().st_size / 1e6}MB")
    # Upon after-crash inspection, clickhouse does seem to atomically run these queries.
    q = (
        f"INSERT INTO {database}.trades\n"
        f"select\n"
        f"  sequence_number as seqnum,\n"
        f"  tape,\n"
        f"  ifNull(toUInt64OrNull(id), sipHash64(id)),\n"
        f"  ticker,\n"
        f"  fromUnixTimestamp64Nano(sip_timestamp) as ts,\n"
        f"  fromUnixTimestamp64Nano(participant_timestamp) as ts_participant,\n"
        f"  fromUnixTimestamp64Nano(trf_timestamp) as ts_trf,\n"
        f"  price,\n"
        f"  size,\n"
        f"  conditions,\n"
        f"  ifNull(correction, 0),\n"
        f"  exchange,\n"
        f"  trf_id as trf\n"
        f"FROM file(\n"
        f"  '{f.path}',\n"
        f"  CSVWithNames,\n"
        f"  `\n"
        f"    ticker                LowCardinality(String),\n"
        f"    conditions            Array(UInt8),\n"
        f"    correction            Nullable(UInt8),\n"
        f"    exchange              UInt8,\n"
        f"    id                    String,\n"
        f"    participant_timestamp UInt64,\n"
        f"    price                 Float64,\n"
        f"    sequence_number       UInt64,\n"
        f"    sip_timestamp         UInt64,\n"
        f"    size                  Float64,\n"
        f"    tape                  UInt8,\n"
        f"    trf_id                Nullable(UInt8),\n"
        f"    trf_timestamp         UInt64\n"
        f"  `\n"
        f")\n"
        f"where not isTestTicker(ticker, ts)"
    )
    return query(q)

def ingest_tickers(f: DirEntry):
    if already_ingested('tickers', f):
        print(f"{f.name} (skip)")
        return
    print(f"{f.name} {f.stat().st_size / 1e6}MB")
    # See polycsv/src/polygon.zig TickerDetails
    q = (
        f"INSERT INTO {database}.tickers\n"
        f"select\n"
        f"  toDate(substring(_file, 1, 10)) as ts,\n"
        f"  *\n"
        f"FROM file(\n"
        f"  '{f.path}',\n"
        f"  CSVWithNames,\n"
        f"  `\n"
        f"ticker                         LowCardinality(String),\n"
        f"active                         Boolean,\n"
        f"name                           LowCardinality(String),\n"
        f"primary_exchange               LowCardinality(String),\n"
        f"list_date                      Nullable(Date32),\n"
        f"delisted_utc                   LowCardinality(String),\n"
        f"description                    LowCardinality(String),\n"
        f"homepage_url                   LowCardinality(String),\n"
        f"address1                       LowCardinality(String),\n"
        f"address2                       LowCardinality(String),\n"
        f"city                           LowCardinality(String),\n"
        f"state                          LowCardinality(String),\n"
        f"country                        LowCardinality(String),\n"
        f"postal_code                    LowCardinality(String),\n"
        f"icon_url                       LowCardinality(String),\n"
        f"logo_url                       LowCardinality(String),\n"
        f"accent_color                   LowCardinality(String),\n"
        f"light_color                    LowCardinality(String),\n"
        f"dark_color                     LowCardinality(String),\n"
        f"cik                            LowCardinality(String),\n"
        f"composite_figi                 LowCardinality(String),\n"
        f"phone_number                   LowCardinality(String),\n"
        f"share_class_figi               LowCardinality(String),\n"
        f"share_class_shares_outstanding Nullable(Float64),\n"
        f"sic_code                       Nullable(UInt16),\n"
        f"sic_description                LowCardinality(String),\n"
        f"total_employees                Nullable(UInt32),\n"
        f"weighted_shares_outstanding    Nullable(Float64)\n"
        f"  `\n"
        f")\n"
        f"where not isTestTicker(ticker, ts)"
    )
    return query(q)

def panic(
    *values: object,
    sep: str | None = " ",
    end: str | None = "\n",
    file = None,
    flush = False,
) -> None:
    print(values, sep, end, file, flush)
    sys.exit(1)

def get_files(d, acc=[]):
    res = acc
    if path.isdir(d):
        with scandir(d) as it:
            for entry in it:
                if entry.is_file() and entry.name.endswith(suffix):
                    res.append(entry)
                elif entry.is_dir():
                    get_files(entry.path, acc)
    res.sort(key=lambda x: x.name)
    return res

def get_user_files_path(clickhouse_config_path):
    if not path.isfile(clickhouse_config_path):
        panic('invalid clickhouse config path', clickhouse_config_path)

    root = ET.parse(clickhouse_config_path).getroot()
    paths = root.findall('user_files_path')
    if len(paths) != 1:
        panic('clickhouse config file MUST include user_files_path')

    user_files_path = paths[0].text

    if type(user_files_path) is not str:
        panic('user_files_path must be a string')

    return str(user_files_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--clickhouse-config', default = '/etc/clickhouse-server/config.xml')
    parser.add_argument('-t', '--tickers-dir', default = 'csv/tickers')
    parser.add_argument('-r', '--trades-dir', default = 'csv/trades')
    args = parser.parse_args()

    user_files_path = get_user_files_path(args.clickhouse_config)
    tickers_dir = path.join(user_files_path, args.tickers_dir)
    trades_dir = path.join(user_files_path, args.trades_dir)

    with ThreadPoolExecutor(max_workers=3) as executor:
        # for f in get_files(tickers_dir):
        #     executor.submit(ingest_tickers, f)

        for f in get_files(trades_dir):
            executor.submit(ingest_trades, f)

