package ingest

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/polygon-io/client-go/rest/models"
)

var tickerCount uint64

func (e *IngestCmd) download_and_flush_tickers(ticker string, date time.Time) {
	params := models.GetTickerDetailsParams{
		Ticker: ticker,
	}.WithDate(models.Date(date))
	details, err := e.polygon.GetTickerDetails(e.ctx, params)
	if err != nil {
		errDetails := err.(*models.ErrorResponse)
		if errDetails.StatusCode == 404 {
			e.logger.Warn(date.Format(dateFormat), " missing ticker details for ", ticker)
		} else {
			panic(err)
		}
	}
	sql := fmt.Sprintf("INSERT INTO %s.tickers", e.viper.GetString("database"))
	batch, err := e.db.PrepareBatch(e.ctx, sql)
	if err != nil {
		panic(err)
	}
	res := details.Results
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
		ticker,
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
	atomic.AddUint64(&tickerCount, 1)
	err = batch.Send()
	if err != nil {
		panic(err)
	}
}

func (e *IngestCmd) worker_tickers(c chan string, wg *sync.WaitGroup, date time.Time) {
	for ticker := range c {
		e.download_and_flush_tickers(ticker, date)
	}
	wg.Done()
}

func (e *IngestCmd) download_day_tickers(date time.Time, tickers []string) error {
	ticker_chan := make(chan string)
	wg := &sync.WaitGroup{}

	for i := 0; i < e.viper.GetInt("max-open-conns"); i++ {
		wg.Add(1)
		go e.worker_tickers(ticker_chan, wg, date)
	}

	for _, ticker := range tickers {
		ticker_chan <- ticker
	}
	close(ticker_chan)
	wg.Wait()

	return nil
}
