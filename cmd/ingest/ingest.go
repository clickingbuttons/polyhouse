package ingest

import (
	"time"
	"os"
	"golang.org/x/exp/slices"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	polygon "github.com/polygon-io/client-go/rest"
	"github.com/clickingbuttons/polyhouse/lib"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const dateFormat = "2006-01-02"

type IngestCmd struct {
	logger *logrus.Entry
	viper *viper.Viper
	db driver.Conn
	polygon *polygon.Client
	tapes map[int]string
	participants map[int]string
}

func NewIngest(logger *logrus.Entry) (*cobra.Command, error) {
	schema := &IngestCmd{
		logger: logger,
		viper:  viper.New(),
	}

	cmd := &cobra.Command{
		Use:               "ingest",
		Short:             "Ingests data from Polygon.io into Clickhouse",
		PersistentPreRunE: schema.persistentPreRun,
		RunE: schema.runE,
	}
	cmd.Flags().String("from", "2004-01-02", "ingest from this date")
	cmd.Flags().String("to", time.Now().AddDate(0, 0, -2).Format(dateFormat), "ingest to this date")
	cmd.Flags().StringArray("tables", []string{"tickers", "trades"}, "tables to ingest data into")

	return cmd, nil
}

func (e *IngestCmd) persistentPreRun(cmd *cobra.Command, args []string) error {
	if err := e.viper.BindPFlags(cmd.PersistentFlags()); err != nil {
		return err
	}
	if err := e.viper.BindPFlags(cmd.Flags()); err != nil {
		return err
	}
	return nil
}

func (e *IngestCmd) ingestTickers() error {
	e.logger.Info("ingest tickers")
	return nil
}

func (e *IngestCmd) runE(cmd *cobra.Command, args []string) error {
	var err error
	e.db, err = lib.MakeClickhouseClient(e.viper)
	if err != nil {
		return err
	}
	e.polygon = polygon.New(os.Getenv("POLY_API_KEY"))
	tables := e.viper.GetStringSlice("tables")

	if slices.Contains(tables, "trades") {
		if err = e.ingestTrades(); err != nil {
			return err
		}
	}

	if slices.Contains(tables, "trades") {
		if err = e.ingestTickers(); err != nil {
			return err
		}
	}

	return nil
}
