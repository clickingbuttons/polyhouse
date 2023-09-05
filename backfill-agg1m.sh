#!/bin/bash

database="us_equities"
d=$(clickhouse-client -q "select toDate(min(min_time)) from system.parts where database='$database' and table='trades'")
d2=$(clickhouse-client -q "select toDate(max(max_time)) from system.parts where database='$database' and table='trades'")
while [ "$d" != "$d2" ]; do
	echo $d
	next=$(date -I -d "$d + 1 day")

	query="insert into agg1m_data SELECT
	toStartOfMinute(ts) AS ts,
	ticker,
	argMinState(price, trades.ts) AS open,
	maxState(price) AS high,
	minState(price) AS low,
	argMaxState(price, trades.ts) AS close,
	sumState(toUInt64(size)) AS volume,
	sumState(price * size) AS liquidity,
	countState() AS count
FROM us_equities.trades
WHERE
	correction=0 AND
	price != 0 AND
	NOT hasAny(conditions, [4, 5, 13, 15, 16, 22, 29, 30, 32, 33, 38]) AND
	ts between '$d' and '$next'
GROUP BY
	ticker,
	ts;"

	if clickhouse-client --query "$query"; then
		d=$next
	else
		echo 'query failed, sleeping 10s'
		sleep 10
	fi
done
