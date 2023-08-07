package schema

import (
	"bytes"
	"context"
	"fmt"
	"golang.org/x/exp/slices"
	"strings"
	"text/template"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/clickingbuttons/polyhouse/lib"
)

type SchemaCmd struct {
	logger    *logrus.Entry
	viper     *viper.Viper
	templates *template.Template
	fields    map[string]interface{}
	db        driver.Conn
}

func NewSchema(logger *logrus.Entry) (*cobra.Command, error) {
	schema := &SchemaCmd{
		logger: logger,
		viper:  viper.New(),
	}

	cmd := &cobra.Command{
		Use:               "schema",
		Aliases:           []string{"schemas"},
		Short:             "Creates Clickhouse schemas for Polygon data",
		PersistentPreRunE: schema.persistentPreRun,
		RunE:              schema.runE,
	}

	cmd.Flags().StringArrayP("table", "t", []string{
		"tickers",
		"trades",
		"agg1d",
		"agg1m",
	}, "tables to create")

	return cmd, nil
}

func (e *SchemaCmd) persistentPreRun(cmd *cobra.Command, args []string) error {
	if err := e.viper.BindPFlags(cmd.PersistentFlags()); err != nil {
		return err
	}
	if err := e.viper.BindPFlags(cmd.Flags()); err != nil {
		return err
	}
	return nil
}

const aggFields = `
ticker LowCardinality(String),
open   Float64,
high   Float64,
low    Float64,
close  Float64,
volume UInt64,
vwap   Float64,
count  UInt32
`

const (
	// condition explanations
	// https://polygon.io/glossary/us/stocks/conditions-indicators
	// page 17 https://www.nyse.com/publicdocs/nyse/data/Daily_TAQ_Client_Spec_v3.0.pdf
	// page 16 https://utpplan.com/doc/utpbinaryoutputspec.pdf

	// condition rules (also compared to Yahoo, Tradingview, Schwab, IBKR, Polygon)
	// page 43 https://utpplan.com/DOC/UtpBinaryOutputSpec.pdf
	// page 64 https://www.ctaplan.com/publicdocs/ctaplan/CTS_Pillar_Output_Specification.pdf
	badPriceConditions = "[2, 7, 21, 37, 15, 20, 16, 29, 52, 53]"
	badVolumeConditions = "[15, 16, 38]"
)

func (e *SchemaCmd) createTable(table string) error {
	e.logger.Info("creating ", table)
	template := e.templates.Lookup(table)
	if template == nil {
		msg := fmt.Sprintf("%s template is empty", table)
		return fmt.Errorf(msg)
	}
	buf := bytes.Buffer{}
	err := template.Execute(&buf, e.fields)
	if err != nil {
		return err
	}
	query := buf.String()
	for _, q := range strings.Split(query, ";") {
		if strings.TrimSpace(q) == "" {
			continue
		}
		err = e.db.Exec(context.Background(), q)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *SchemaCmd) maybeCreateTable(table string, tables []string) error {
	if slices.Contains(tables, table) {
		e.fields["table"] = table
		return e.createTable(table)
	}

	return nil
}

func (e *SchemaCmd) runE(cmd *cobra.Command, args []string) error {
	var err error
	e.db, err = lib.MakeClickhouseClient(e.viper)
	if err != nil {
		return err
	}
	e.templates, err = template.ParseGlob(e.viper.GetString("templates"))
	if err != nil {
		return err
	}

	tapeLines := []string{}
	for key, val := range lib.Tapes {
		line := fmt.Sprintf("'%s' = %d", key, val)
		tapeLines = append(tapeLines, line)
	}
	participantsLines := []string{}
	for key, val := range lib.Participants {
		line := fmt.Sprintf("'%s' = %d", key, val)
		participantsLines = append(participantsLines, line)
	}
	e.fields = map[string]interface{}{
		"database":                e.viper.GetString("database"),
		"participants":            strings.Join(participantsLines, ","),
		"tapes":                   strings.Join(tapeLines, ","),
		"aggFields":               aggFields,
		"badPriceConditions":      badPriceConditions,
		"badVolumeConditions":     badVolumeConditions,
	}
	tables := e.viper.GetStringSlice("table")

	if err = e.createTable("database"); err != nil {
		return err
	}

	for _, t := range tables {
		if err = e.maybeCreateTable(t, tables); err != nil {
			return err
		}
	}

	return nil
}
