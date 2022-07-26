# polyhouse

Pretty rough state, but with this schema it downloads trades:
```sql
create database if not exists us_equities;
create table us_equities.trades (
		sequence_number  UInt64,
		tape             UInt8,
		id               UInt64,
		ticker           LowCardinality(String),
		time             Datetime64(9, 'America/New_York'),
		time_participant Nullable(Datetime64(9, 'America/New_York')),
		time_trf         Nullable(Datetime64(9, 'America/New_York')),
		price            Float64,
		size             UInt32,
		conditions       Array(UInt8),
		correction       UInt8,
		exchange         UInt8,
		trf              UInt8,
		update_intraday  Bool materialized updateIntraday(price, size, conditions, correction),
		update_day       Bool materialized updateDay(price, size, conditions, correction)
	)
	Engine = MergeTree
	partition by toYYYYMMDD(time)
	order by (ticker, time);
```
