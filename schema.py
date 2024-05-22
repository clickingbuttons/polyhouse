from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
import csv

database = 'us_equities'

ns_ty = "Datetime64(9, 'America/New_York')"
s_ty = "DateTime('America/New_York')"

tapes = """
'A-NYSE' = 1,
'B-ARCA' = 2,
'C-NASD' = 3
"""

participants = """
'None'                                           = 0,
'NYSE American LLC'                              = 1,
'Nasdaq OMX BX Inc.'                             = 2,
'NYSE National Inc.'                             = 3,
'FINRA Alternative Display Facility'             = 4,
'Unlisted Trading Privileges'                    = 5,
'International Securities Exchange LLC - Stocks' = 6,
'Cboe EDGA'                                      = 7,
'Cboe EDGX'                                      = 8,
'NYSE Chicago Inc.'                              = 9,
'New York Stock Exchange'                        = 10,
'NYSE Arca Inc.'                                 = 11,
'Nasdaq'                                         = 12,
'Consolidated Tape Association'                  = 13,
'Long-Term Stock Exchange'                       = 14,
'Investors Exchange'                             = 15,
'Cboe Stock Exchange'                            = 16,
'Nasdaq Philadelphia Exchange LLC'               = 17,
'Cboe BYX'                                       = 18,
'Cboe BZX'                                       = 19,
'MIAX Pearl'                                     = 20,
'Members Exchange'                               = 21,
'OTC Equity Security'                            = 62
"""

# condition explanations
# https://polygon.io/glossary/us/stocks/conditions-indicators
# page 17 https://www.nyse.com/publicdocs/nyse/data/Daily_TAQ_Client_Spec_v3.0.pdf
# page 16 https://utpplan.com/doc/utpbinaryoutputspec.pdf
#
# upstream condition rules
# page 43 https://utpplan.com/DOC/UtpBinaryOutputSpec.pdf
# page 64 https://www.ctaplan.com/publicdocs/ctaplan/CTS_Pillar_Output_Specification.pdf
#
# Reduce noise by filtering some trades.
# - Filter trades executed at times other than their timestamp.
# - Filter trades that a retail investor could not make.
#
# 4, 5 - Bunched trade
# Bunched trades are reported AFTER the entire trade is completed. Furthermore, they disappear
# around 2007.
# 13 - Sold out of sequence (reported time different from transaction time)
# 15, 16 - Market center official open/close (not a real trade...)
# 22 - Prior reference price (>90s)
# 29 - Seller's option (can deliver between 2-60 days)
# 30 - Sold last (late but in sequence)
# 32, 33 - Sold (out of sequence, late)
# 38 - Corrected consolidated close (not a real trade...)
bad_conditions = '[4, 5, 13, 15, 16, 22, 29, 30, 32, 33, 38]'

def trade_columns():
    return (
        f"  seqnum          UInt64,\n"
        f"  tape            Enum8({tapes}),\n"
        f"  id              UInt64,\n"
        f"  ticker          LowCardinality(String),\n"
        f"  ts              {ns_ty},\n"
        f"  ts_participant  Nullable({ns_ty}),\n"
        f"  ts_trf          Nullable({ns_ty}),\n"
        f"  price           Float64,\n"
        f"  size            UInt32,\n"
        f"  conditions      Array(UInt8),\n"
        f"  correction      UInt8,\n"
        f"  exchange        Enum8({participants}),\n"
        f"  trf             Enum8({participants})\n"
    )

def trades_table():
    return (
        f"CREATE TABLE {database}.trades (\n"
        f"{trade_columns()}"
        f")\n"
        f"Engine = MergeTree()\n"
        f"PARTITION BY toYYYYMMDD(ts)\n"
        f"PRIMARY KEY (ticker, ts)\n"
        f"ORDER BY (ticker, ts, seqnum)\n"
    )

def agg_table(name, ts_ty, partition, from_ty):
    part = f"PARTITION BY {partition}(ts)" if partition != 'none' else ''
    return (
        f"CREATE TABLE {database}.{name}_data (\n"
        f"  ts         {ts_ty},\n"
        f"  ticker     LowCardinality(String),\n"
        f"  open       AggregateFunction(argMin, Float64, {from_ty}),\n"
        f"  high       AggregateFunction(max, Float64),\n"
        f"  low        AggregateFunction(min, Float64),\n"
        f"  close      AggregateFunction(argMax, Float64, {from_ty}),\n"
        f"  volume     AggregateFunction(sum, UInt64),\n"
        f"  liquidity  AggregateFunction(sum, Float64),\n"
        f"  count      AggregateFunction(count, UInt32)\n"
        f")\n"
        f"Engine = AggregatingMergeTree\n"
        f"{part}\n"
        f"PRIMARY KEY (ticker, ts)\n"
        f"ORDER BY (ticker, ts)"
    )

def agg_mv_from_trades(name, interval):
    return (
        f"CREATE MATERIALIZED VIEW {database}.{name}_mv\n"
        f"TO {database}.{name}_data\n"
        f"AS\n"
        f"SELECT\n"
        f"  toStartOfInterval(ts, interval {interval}) AS ts,\n"
        f"  ticker,\n"
        f"  argMinState(price, trades.ts) AS open,\n"
        f"  maxState(price) AS high,\n"
        f"  minState(price) AS low,\n"
        f"  argMaxState(price, trades.ts) AS close,\n"
        f"  sumState(toUInt64(size)) AS volume,\n"
        f"  sumState(price * size) AS liquidity,\n"
        f"  countState() AS count\n"
        f"FROM trades\n"
        f"WHERE correction=0 AND price!=0 AND NOT hasAny(conditions, {bad_conditions})\n"
        f"GROUP BY\n"
        f"  ticker,\n"
        f"  ts"
    )

def agg_mv_from_aggs(name, interval, agg):
    # https://clickhouse.com/docs/en/guides/developer/cascading-materialized-views#yearly-aggregated-table-and-materialized-view
    return (
        f"CREATE MATERIALIZED VIEW {database}.{name}_mv\n"
        f"TO {database}.{name}_data\n"
        f"AS\n"
        f"SELECT\n"
        f"  toStartOfInterval(ts, interval {interval}) AS ts,\n"
        f"  ticker,\n"
        f"  argMinState(finalizeAggregation(open), other.ts) AS open,\n"
        f"  maxMergeState(high) AS high,\n"
        f"  minMergeState(low) AS low,\n"
        f"  argMaxState(finalizeAggregation(close), other.ts) AS close,\n"
        f"  sumMergeState(volume) as volume,\n"
        f"  sumMergeState(liquidity) as liquidity,\n"
        f"  countMergeState(count) as count\n"
        f"FROM {database}.{agg}_data as other\n"
        f"GROUP BY\n"
        f"  ticker,\n"
        f"  ts"
    )

def agg_mv_view(name):
    return (
        f"CREATE VIEW {name}\n"
        f"AS\n"
        f"SELECT\n"
        f"  ts,\n"
        f"  ticker,\n"
        f"  argMinMerge(open) AS open,\n"
        f"  maxMerge(high) AS high,\n"
        f"  minMerge(low) AS low,\n"
        f"  argMaxMerge(close) AS close,\n"
        f"  sumMerge(volume) as volume,\n"
        f"  sumMerge(liquidity) as liquidity,\n"
        f"  liquidity / volume as vwap,\n"
        f"  countMerge(count) as count\n"
        f"FROM {name}_mv\n"
        f"GROUP BY\n"
        f"  ticker,\n"
        f"  ts"
  );

def tickers_table():
    return (
        f"CREATE TABLE {database}.tickers (\n"
        f"ts                             Date32,\n"
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
        f")\n"
        f"Engine = MergeTree\n"
        f"PARTITION BY toYear(ts)\n"
        f"ORDER BY (ticker, ts)"
    )

def test_tickers_udf():
    reader = csv.DictReader(open('./test_tickers.csv', 'r'))
    suffix = '(p[A-Z]?|(\\.WS)?\\.[A-Z]|p?\\.WD|\\.[A-Z]|p?\\.[A-Z]?CL|p[A-Z]w|\\.EC|\\.PP||\\.CV||\\.[A-Z]CV|p[A-Z]\\.(CV|WD)|r|\\.U|r?p?w|\\.Aw|\\.WSw)?'
    conditions = []
    for row in reader:
        condition = f"match(t, '{row['regex']}{suffix}')"
        if row['from'] != '':
            condition += f" and '{row['from']}' < d"
        if row['to'] != '':
            condition += f" and d < '{row['to']}'"
        conditions.append(f"({condition})")
    return (
       f"create function isTestTicker as (t, d) -> {' or '.join(conditions)}"
    )

def query(q, debug=True):
    try:
        request = Request('http://localhost:8123')
        with urlopen(request, data=q.encode('utf-8')) as response:
            return response.read().decode('utf-8').strip()
    except HTTPError as error:
        print(error.status, error.reason)
        print(error.read().decode('utf-8').strip())
        if debug:
            print(q)
    except URLError as error:
        print(error.reason)
    except TimeoutError:
        print("Request timed out")

aggs = {
        'agg1s': {
            'ty': s_ty,
            'partition': 'toYYYYMMDD',
            'interval': '1 second',
        },
        'agg1m': {
            'ty': s_ty,
            'partition': 'toYYYYMMDD',
            'interval': '1 minute',
        },
        'agg30m': {
            'ty': s_ty,
            'partition': 'toYYYYMM',
            'interval': '30 minutes',
        },
        'agg1d': {
            'ty': 'Date',
            'partition': 'toYear',
            'interval': '1 day',
        },
        'agg1mo': {
            'ty': 'Date',
            'partition': 'none',
            'interval': '1 month',
        },
        'agg1y': {
            'ty': 'Date',
            'partition': 'none',
            'interval': '1 year',
        },
}

def main():
    query(f"drop database if exists {database}");
    query(f"create database if not exists {database}");
    query(f"drop function if exists isTestTicker");

    query(tickers_table())
    query(test_tickers_udf())
    query(trades_table())

    for i, (k, v) in enumerate(aggs.items()):
        if i == 0:
            query(agg_table(k, v['ty'], v['partition'], ns_ty))
            query(agg_mv_from_trades(k, v['interval']))
        else:
            query(agg_table(k, v['ty'], v['partition'], list(aggs.values())[i - 1]["ty"]))
            query(agg_mv_from_aggs(k, v['interval'], list(aggs.keys())[i - 1]))

        query(agg_mv_view(k))

if __name__ == '__main__':
    query(tickers_table())
   # main()
