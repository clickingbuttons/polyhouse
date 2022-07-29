package schema

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"text/template"
	"golang.org/x/exp/slices"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/clickingbuttons/polyhouse/lib"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Schema struct {
	logger *logrus.Entry
	viper *viper.Viper
}

func NewSchema(logger *logrus.Entry) (*cobra.Command, error) {
	schema := &Schema{
		logger: logger,
		viper:  viper.New(),
	}

	cmd := &cobra.Command{
		Use:               "schema",
		Short:             "Creates Polygon schemas",
		PersistentPreRunE: schema.persistentPreRun,
		RunE: schema.runE,
	}

	cmd.Flags().String("database", "us_equities", "name of database to create")
	cmd.Flags().StringArray("tables", []string{
		"tickers",
		"trades",
		"agg1m", "agg1d", "agg1d_intra",
		"poly_agg1m", "poly_agg1d", "poly_agg1d_intra",
	}, "tables to create")

	return cmd, nil
}

func (e *Schema) persistentPreRun(cmd *cobra.Command, args []string) error {
	if err := e.viper.BindPFlags(cmd.PersistentFlags()); err != nil {
		return err
	}
	if err := e.viper.BindPFlags(cmd.Flags()); err != nil {
		return err
	}
	return nil
}

func (e *Schema) makeTickers(clickhouseClient driver.Conn) error {
	schema := fmt.Sprintf(`
	CREATE TABLE %s.tickers (
		ticker													LowCardinality(String),
		ts															Date32,
		name														LowCardinality(String),
		primary_exchange								LowCardinality(String),
		type														LowCardinality(String),
		cik															LowCardinality(String),
		composite_figi									LowCardinality(String),
		share_class_figi								LowCardinality(String),
		phone_number										LowCardinality(String),
		description											LowCardinality(String),
		sic_code												Nullable(UInt16),
		ticker_root											LowCardinality(String),
		homepage_url										LowCardinality(String),
		total_employees									Nullable(UInt32),
		list_date												Nullable(Date),
		share_class_shares_outstanding	Nullable(Float64),
		weighted_shares_outstanding			Nullable(Float64)
	)
	Engine = MergeTree
	PARTITION BY toYear(ts)
	ORDER BY (ticker, ts)
	`, e.viper.GetString("database"))


	return clickhouseClient.Exec(context.Background(), schema)
}

func (e *Schema) makeTrades(clickhouseClient driver.Conn) error {
	// page 43 https://utpplan.com/DOC/UtpBinaryOutputSpec.pdf
	// page 64 https://www.ctaplan.com/publicdocs/ctaplan/CTS_Pillar_Output_Specification.pdf
	badConditions := []string{"2", "7", "21", "37", "15", "20", "16", "29", "52", "53"}
	badVolumeConditions := []string{"15", "16", "38"}
	// from looking at TAQ + UTP + CTA
	// page 17 https://www.nyse.com/publicdocs/nyse/data/Daily_TAQ_Client_Spec_v3.0.pdf
	// page 16 https://utpplan.com/doc/utpbinaryoutputspec.pdf
	polyBadConditions := []string{"10", "15", "16", "17", "18", "19", "21", "22", "23", "24", "29", "30", "33", "38", "40", "46", "52", "53"}
	polyBadVolumeConditions := []string{}
	participants := map[string]int {
		"NYSE American, LLC": 1,
		"Nasdaq OMX BX, Inc.": 2,
		"NYSE National, Inc.": 3,
		"FINRA Alternative Display Facility": 4,
		"Unlisted Trading Privileges": 5,
		"International Securities Exchange, LLC - Stocks": 6,
		"Cboe EDGA": 7,
		"Cboe EDGX": 8,
		"NYSE Chicago, Inc.": 9,
		"New York Stock Exchange": 10,
		"NYSE Arca, Inc.": 11,
		"Nasdaq": 12,
		"Consolidated Tape Association": 13,
		"Long-Term Stock Exchange": 14,
		"Investors Exchange": 15,
		"Cboe Stock Exchange": 16,
		"Nasdaq Philadelphia Exchange LLC": 17,
		"Cboe BYX": 18,
		"Cboe BZX": 19,
		"MIAX Pearl": 20,
		"Members Exchange": 21,
		"OTC Equity Security": 62,
	}
	participantsLines := []string{}
	for key, val := range participants {
		line := fmt.Sprintf("'%s' = %d", key, val)
		participantsLines = append(participantsLines, line)
	}

	myTemplate, err := template.New("trades").Parse(`
	CREATE TABLE {{ .database }}.trades
	(
		seqnum								UInt64,
		tape             			Enum8('A-NYSE', 'B-ARCA', 'C-NASD'),
		id               			UInt64,
		ticker           			LowCardinality(String),
		ts               			Datetime64(9, 'America/New_York'),
		ts_participant   			Nullable(Datetime64(9, 'America/New_York')),
		ts_trf           			Nullable(Datetime64(9, 'America/New_York')),
		price            			Float64,
		size             			UInt32,
		conditions       			Array(UInt8),
		correction       			UInt8,
		exchange         			Enum8({{ .participants }}),
		trf              			Enum8({{ .participants }}),
		update_intraday  			Bool materialized correction=0 AND NOT hasAny(conditions, [{{ .badConditions }}]),
		update_day       			Bool materialized correction=0 AND NOT hasAny(conditions, [{{ .badConditions }}]) AND NOT hasAny(conditions, [12]),
		update_volume					Bool materialized correction=0 AND NOT hasAny(conditions, [{{ .badVolumeConditions }}]),
		poly_update_intraday  Bool materialized correction=0 AND NOT hasAny(conditions, [{{ .polyBadConditions }}]),
		poly_update_day       Bool materialized correction=0 AND NOT hasAny(conditions, [{{ .polyBadConditions }}]) AND NOT hasAny(conditions, [12]),
		poly_update_volume		Bool materialized correction=0 AND NOT hasAny(conditions, [{{ .polyBadVolumeConditions }}])
	)
	Engine = MergeTree
	partition by toYYYYMMDD(ts)
	order by (ticker, ts);
	`)
	if err != nil {
		return err
	}
	buf := bytes.Buffer{}
	myTemplate.Execute(&buf, map[string]interface{}{
		"participants": strings.Join(participantsLines, ","),
		"database": e.viper.GetString("database"),
		"badConditions": strings.Join(badConditions, ","),
		"badVolumeConditions": strings.Join(badVolumeConditions, ","),
		"polyBadConditions": strings.Join(polyBadConditions, ","),
		"polyBadVolumeConditions": strings.Join(polyBadVolumeConditions, ","),
	})

	return clickhouseClient.Exec(context.Background(), buf.String())
}

const aggFields = `
	ticker	LowCardinality(String),
	open		Float64,
	high		Float64,
	low			Float64,
	close		Float64,
	volume  UInt64,
	vwap		Float64,
	count		UInt32
`

func (e *Schema) makeAgg1m(clickhouseClient driver.Conn) error {
	database := e.viper.GetString("database")
	schema := fmt.Sprintf(`
	CREATE TABLE %s.agg1m (
		ts			DateTime('America/New_York'),
		%s
	)
	ENGINE = AggregatingMergeTree
	PARTITION BY toYYYYMM(ts)
	ORDER BY (ticker, ts)
	`, database, aggFields);
	err := clickhouseClient.Exec(context.Background(), schema)
	if err != nil {
		return err
	}

	view := fmt.Sprintf(`
	CREATE MATERIALIZED VIEW %s.agg1m_agger
	TO %s.agg1m
	AS SELECT
		ticker,
		toStartOfMinute(ts) AS ts,
		any(price) AS open,
		max(price) AS high,
		min(price) AS low,
		anyLast(price) AS close,
		sum(size) AS volume,
		sum(price * size) / sum(size) AS vwap,
		count() AS count
	FROM us_equities.trades
	WHERE update_intraday = true
	GROUP BY
			ticker,
			ts
	`, database, database)

	return clickhouseClient.Exec(context.Background(), view)
}

func (e *Schema) makeAgg1d(clickhouseClient driver.Conn) error {
	database := e.viper.GetString("database")
	schema := fmt.Sprintf(`
	CREATE TABLE %s.agg1d (
		ts			Date32,
		%s
	)
	ENGINE = AggregatingMergeTree
	PARTITION BY toYYYYMM(ts)
	ORDER BY (ticker, ts)
	`, database, aggFields);
	err := clickhouseClient.Exec(context.Background(), schema)
	if err != nil {
		return err
	}

	view := fmt.Sprintf(`
	CREATE MATERIALIZED VIEW %s.agg1d_agger
	TO %s.agg1d
	AS SELECT
		ticker,
		toDate32(toStartOfDay(ts)) AS ts,
		any(price) AS open,
		max(price) AS high,
		min(price) AS low,
		anyLast(price) AS close,
		sum(size) AS volume,
		sum(price * size) / sum(size) AS vwap,
		count() AS count
	FROM us_equities.trades
	WHERE update_day = true
	GROUP BY
		ticker,
		ts
	`, database, database)

	return clickhouseClient.Exec(context.Background(), view)
}

func (e *Schema) runE(cmd *cobra.Command, args []string) error {
	tables := e.viper.GetStringSlice("tables")
	clickhouseClient, err := lib.MakeClickhouseClient(e.viper)
	if err != nil {
		return err
	}
	database := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", e.viper.GetString("database"))
	err = clickhouseClient.Exec(context.Background(), database)
	if err != nil {
		return err
	}
	e.logger.Info(tables)

	if slices.Contains(tables, "tickers") {
		err = e.makeTickers(clickhouseClient)
		if err != nil {
			return err
		}
	}

	if slices.Contains(tables, "trades") {
		err = e.makeTrades(clickhouseClient)
		if err != nil {
			return err
		}
	}

	if slices.Contains(tables, "agg1m") {
		err = e.makeAgg1m(clickhouseClient)
		if err != nil {
			return err
		}
	}

	if slices.Contains(tables, "agg1d") {
		err = e.makeAgg1d(clickhouseClient)
		if err != nil {
			return err
		}
	}

	return nil
}
