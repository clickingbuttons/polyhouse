#!/bin/bash

database="us_equities"
d="2020-07-02"
d2=$(clickhouse-client -q "select toDate(max(ts)) from $database.trades")
while [ "$d" != "$d2" ]; do
	next=$(date -I -d "$d + 1 day")
	clickhouse-client --query "
WITH
	arrayFlatten(groupArrayIf(conditions, notEmpty(conditions))) AS conds,
	arrayReduce('groupUniqArray', conds) AS uniqconds,
	arraySort(x -> x.1, arrayMap(x -> (x, countEqual(conds, x)), uniqconds)) AS resultconds
SELECT
	toYYYYMMDD(ts) AS period,
	resultconds,
	count() AS count
FROM trades
WHERE ts between '$d' and '$next'
GROUP BY period
FORMAT CSV"
	d=$next
done
