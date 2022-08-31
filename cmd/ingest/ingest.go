package ingest

import (
	"bufio"
	"context"
	"golang.org/x/exp/slices"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/clickingbuttons/polyhouse/lib"
	polygon "github.com/polygon-io/client-go/rest"
	"github.com/polygon-io/client-go/rest/models"
)

const dateFormat = "2006-01-02"

type IngestCmd struct {
	logger           *logrus.Entry
	viper            *viper.Viper
	db               driver.Conn
	polygon          *polygon.Client
	tapes            map[int]string
	participants     map[int]string
	blacklistTickers []string
	ctx              context.Context
}

func NewIngest(logger *logrus.Entry) (*cobra.Command, error) {
	schema := &IngestCmd{
		logger:           logger,
		viper:            viper.New(),
		blacklistTickers: []string{},
		ctx:              context.Background(),
	}

	cmd := &cobra.Command{
		Use:               "ingest",
		Short:             "Ingests data from Polygon.io into Clickhouse",
		PersistentPreRunE: schema.persistentPreRun,
		RunE:              schema.runE,
	}
	cmd.Flags().String("from", "2004-01-02", "ingest from this date")
	cmd.Flags().String("to", time.Now().AddDate(0, 0, -3).Format(dateFormat), "ingest to this date")
	cmd.Flags().String("blacklist-file", "./test_tickers.txt", "newline separated list of tickers to ignore")
	cmd.Flags().StringArrayP("tables", "t", []string{"tickers", "trades"}, "tables to ingest data into")

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

func (e *IngestCmd) init() (from, to time.Time, err error) {
	// parse dates
	from, err = time.Parse(dateFormat, e.viper.GetString("from"))
	if err != nil {
		return from, to, err
	}
	to, err = time.Parse(dateFormat, e.viper.GetString("to"))
	if err != nil {
		return from, to, err
	}

	// load bad tickers
	file, err := os.Open(e.viper.GetString("blacklist-file"))
	if err != nil {
		return from, to, err
	}
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		e.blacklistTickers = append(e.blacklistTickers, scanner.Text())
	}
	e.logger.Info("read ", len(e.blacklistTickers), " tickers from blacklist file")

	// invert maps for clickhouse-go :(
	e.tapes = map[int]string{}
	for key, val := range lib.Tapes {
		e.tapes[val] = key
	}
	e.participants = map[int]string{}
	for key, val := range lib.Participants {
		e.participants[val] = key
	}

	return from, to, err
}

func (e *IngestCmd) getTickers(d time.Time) ([]string, error) {
	params := models.GetGroupedDailyAggsParams{
		Locale:     models.MarketLocale("us"),
		MarketType: models.MarketType("stocks"),
		Date:       models.Date(d),
	}.WithAdjusted(false)
	tickers, err := e.polygon.AggsClient.GetGroupedDailyAggs(e.ctx, params)
	if err != nil {
		return []string{}, err
	}
	if tickers.ResultsCount == 0 {
		return []string{}, nil
	}
	tickerList := []string{}
	for _, t := range tickers.Results {
		if !slices.Contains(e.blacklistTickers, t.Ticker) {
			tickerList = append(tickerList, t.Ticker)
		}
	}

	return tickerList, nil
}

func (e *IngestCmd) runE(cmd *cobra.Command, args []string) error {
	var err error
	e.db, err = lib.MakeClickhouseClient(e.viper)
	if err != nil {
		return err
	}
	e.polygon = polygon.New(os.Getenv("POLY_API_KEY"))
	tables := e.viper.GetStringSlice("tables")

	from, to, err := e.init()
	if err != nil {
		return err
	}

	for d := to; d.After(from); d = d.AddDate(0, 0, -1) {
		tickerCount = 0
		tradeCount = 0
		begin := time.Now()

		tickers, err := e.getTickers(d)
		if len(tickers) == 0 {
			e.logger.Info(d.Format("2006-01-02"), " no data")
			continue
		}
		e.logger.Infof("ingesting %d tickers for %s", len(tickers), d.Format(dateFormat))

		if slices.Contains(tables, "tickers") {
			if err = e.download_day_tickers(time.Time(d), tickers); err != nil {
				return err
			}
		}
		if slices.Contains(tables, "trades") {
			if err = e.download_day_trades(time.Time(d), tickers); err != nil {
				return err
			}
		}

		elapsed := time.Since(begin)
		e.logger.Infof("%s took %s for %d tickers and %d trades\n", d.Format("2006-01-02"), elapsed, tickerCount, tradeCount)
	}

	return nil
}
