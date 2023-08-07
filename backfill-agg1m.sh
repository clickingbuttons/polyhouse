#!/bin/bash

d=2003-09-10
d2=$(date +%Y-%m-%d)
while [ "$d" != "$d2" ]; do
  echo $d
  next=$(date -I -d "$d + 1 day")

	clickhouse-client --query "
insert into agg1m SELECT
	toStartOfMinute(ts) AS ts,
	ticker,
	any(price) AS open,
	max(price) AS high,
	min(price) AS low,
	anyLast(price) AS close,
	sum(size) AS volume,
	sum(price * size) / volume AS vwap,
	count() AS count
FROM us_equities.trades
WHERE
	correction=0 AND
	price != 0 AND
	NOT hasAny(conditions, [2, 7, 21, 37, 15, 20, 16, 29, 52, 53, 38]) AND
	ts between '$d' and '$next'
GROUP BY
	ticker,
	ts;"
	d=$next
done
