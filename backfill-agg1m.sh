#!/bin/bash

d=2003-09-10
d2=$(date +%Y-%m-%d)
while [ "$d" != "$d2" ]; do 
  echo $d
  next=$(date -I -d "$d + 1 day")

	clickhouse-client --query "insert into us_equities.agg1m WITH
             NOT hasAny(conditions, [2, 7, 21, 37, 15, 20, 16, 29, 52, 53]) AS update_price,
             NOT hasAny(conditions, [15, 16, 38]) AS update_volume
         SELECT
             toStartOfMinute(ts) AS ts,
             ticker,
             anyIf(price, update_price) AS open,
             maxIf(price, update_price) AS high,
             minIf(price, update_price) AS low,
             anyLastIf(price, update_price) AS close,
             sumIf(size, update_volume) AS volume,
             sumIf(price * size, update_volume) / volume AS vwap,
             count() AS count
         FROM us_equities.trades
         WHERE correction = 0 and ts between '$d' and '$next'
         GROUP BY
             ticker,
             ts"

	d=$next
done
