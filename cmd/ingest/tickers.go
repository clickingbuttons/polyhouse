package ingest

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/polygon-io/client-go/rest/models"
	"golang.org/x/exp/slices"
)

var tickerCount uint64

func (e *IngestCmd) download_and_flush_tickers(ticker string, date time.Time, results chan models.Ticker) {
	params := models.GetTickerDetailsParams{
		Ticker: ticker,
	}.WithDate(models.Date(date))
	details, err := e.polygon.GetTickerDetails(e.ctx, params)
	if err != nil {
		errDetails, ok := err.(*models.ErrorResponse)
		if !ok {
			panic(err)
		}
		if errDetails.StatusCode == 404 {
			e.logger.Warn(date.Format(dateFormat), " missing ticker details for ", ticker)
		} else {
			panic(err)
		}
	}
	results <- details.Results
	atomic.AddUint64(&tickerCount, 1)
}

func (e *IngestCmd) worker_tickers(c chan string, wg *sync.WaitGroup, date time.Time, results chan models.Ticker) {
	for ticker := range c {
		e.download_and_flush_tickers(ticker, date, results)
	}
	wg.Done()
}

func (e *IngestCmd) get_existing_tickers(date time.Time) ([]string, error) {
	res := []string{}

	cmd := fmt.Sprintf("SELECT ticker from %s.tickers where ts='%s'", e.viper.GetString("database"), date.Format(dateFormat))
	rows, err := e.db.Query(e.ctx, cmd)
	if err != nil {
		return res, err
	}
	for rows.Next() {
		var ticker string
		if err := rows.Scan(&ticker); err != nil {
			return res, nil
		}
		res = append(res, ticker)
	}

	return res, nil
}

func (e *IngestCmd) flush(tickers chan models.Ticker, date time.Time) error {
	sql := fmt.Sprintf("INSERT INTO %s.tickers", e.viper.GetString("database"))
	batch, err := e.db.PrepareBatch(e.ctx, sql)
	if err != nil {
		panic(err)
	}

	for res := range tickers {
		if res.Ticker == "" {
			// was missing details for this day
			continue
		}
		var sicCode *uint16
		if res.SICCode != "" {
			code, err := strconv.ParseUint(res.SICCode, 10, 16)
			if err != nil {
				panic(err)
			}
			code2 := uint16(code)
			sicCode = &code2
		}
		err = batch.Append(
			date,
			res.Ticker,
			res.Name,
			res.PrimaryExchange,
			res.Type,
			res.CIK,
			res.CompositeFIGI,
			res.ShareClassFIGI,
			res.PhoneNumber,
			res.Description,
			sicCode,
			res.TickerRoot,
			res.HomepageURL,
			uint32(res.TotalEmployees),
			parse_time_unix(time.Time(res.ListDate)),
			float64(res.ShareClassSharesOutstanding),
			float64(res.WeightedSharesOutstanding),
			res.Address.Address1,
			res.Address.Address2,
			res.Address.City,
			res.Address.PostalCode,
			res.Address.State,
		)
		if err != nil {
			panic(err)
		}
	}

	err = batch.Send()
	if err != nil {
		panic(err)
	}

	return nil
}

func (e *IngestCmd) download_day_tickers(date time.Time, tickers []string) error {
	toDownload := []string{}
	e.logger.Info(date.Format(dateFormat), " getting already downloaded tickers")
	alreadyDownloaded, err := e.get_existing_tickers(date)
	if err != nil {
		return err
	}
	for _, t := range tickers {
		if !slices.Contains(alreadyDownloaded, t) {
			toDownload = append(toDownload, t)
		}
	}

	e.logger.Info(date.Format(dateFormat), " downloading ", len(tickers), " tickers")
	inputs := make(chan string)
	outputs := make(chan models.Ticker, len(toDownload))
	wg := &sync.WaitGroup{}

	for i := 0; i < e.viper.GetInt("max-open-conns"); i++ {
		wg.Add(1)
		go e.worker_tickers(inputs, wg, date, outputs)
	}

	for _, ticker := range toDownload {
		inputs <- ticker
	}
	close(inputs)
	wg.Wait()

	close(outputs)
	e.logger.Info(date.Format(dateFormat), " done downloading")
	e.flush(outputs, date)

	return nil
}
