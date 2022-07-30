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

type SchemaCmd struct {
	logger *logrus.Entry
	viper *viper.Viper
	templates *template.Template
	fields map[string]interface{}
	db driver.Conn
}

func NewSchema(logger *logrus.Entry) (*cobra.Command, error) {
	schema := &SchemaCmd{
		logger: logger,
		viper:  viper.New(),
	}

	cmd := &cobra.Command{
		Use:               "schema",
		Short:             "Creates Clickhouse schemas for Polygon data",
		PersistentPreRunE: schema.persistentPreRun,
		RunE: schema.runE,
	}

	cmd.Flags().StringArray("tables", []string{
		"tickers",
		"trades",
		"agg1m", "agg1d", "agg1d_intra",
		"cons_agg1m", "cons_agg1d", "cons_agg1d_intra",
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
	// from looking at TAQ + UTP + CTA
	// page 17 https://www.nyse.com/publicdocs/nyse/data/Daily_TAQ_Client_Spec_v3.0.pdf
	// page 16 https://utpplan.com/doc/utpbinaryoutputspec.pdf
	badConditions = "[10, 15, 16, 17, 18, 19, 21, 22, 23, 24, 29, 30, 33, 38, 40, 46, 52, 53]"
	badVolumeConditions = "[]"
	// page 43 https://utpplan.com/DOC/UtpBinaryOutputSpec.pdf
	// page 64 https://www.ctaplan.com/publicdocs/ctaplan/CTS_Pillar_Output_Specification.pdf
	consBadConditions = "[2, 7, 21, 37, 15, 20, 16, 29, 52, 53]"
	consBadVolumeConditions = "[15, 16, 38]"
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

func (e *SchemaCmd) maybeCreateTable(table string) error {
	if slices.Contains(e.viper.GetStringSlice("tables"), table) {
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
		"database": e.viper.GetString("database"),
		"cluster": e.viper.GetString("cluster"),
		"participants": strings.Join(participantsLines, ","),
		"tapes": strings.Join(tapeLines, ","),
		"aggFields": aggFields,
		"badConditions": badConditions,
		"badVolumeConditions": badVolumeConditions,
		"consBadConditions": consBadConditions,
		"consBadVolumeConditions": consBadVolumeConditions,
	}
	e.logger.Info(e.viper.GetStringSlice("tables"))

	if err = e.createTable("database"); err != nil {
		return err
	}

	if err = e.maybeCreateTable("tickers"); err != nil {
		return err
	}

	if err = e.maybeCreateTable("trades"); err != nil {
		return err
	}

	if err = e.maybeCreateTable("agg1m"); err != nil {
		return err
	}

	if err = e.maybeCreateTable("agg1d"); err != nil {
		return err
	}

	if err = e.maybeCreateTable("agg1d_intra"); err != nil {
		return err
	}

	return nil
}
