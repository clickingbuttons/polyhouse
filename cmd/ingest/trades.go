package ingest

import (
	"strconv"
	"sync/atomic"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/clickingbuttons/polyhouse/lib"
	"github.com/polygon-io/client-go/rest/models"
)

var tradeCount uint64

func id_to_u64(id string) uint64 {
	if len(id) <= 8 {
		val := make([]byte, 8)
		copy(val, id)
		return binary.LittleEndian.Uint64(val)
	} else if len(id) <= 20 {
		number, err := strconv.ParseUint(id, 10, 64)	
		if err != nil {
			panic(err)
		}
		return number
	}

	panic(id)
}

func convert_to_u8(ar []int32) []uint8 {
	newar := make([]uint8, len(ar))
	var v int32
	var i int
	for i, v = range ar {
		newar[i] = uint8(v)
	}
	return newar
}

func parse_time(t time.Time) *time.Time {
	var default_time time.Time
	if t == default_time {
		return nil
	}
	return &t
}

func (e *IngestCmd) download_and_flush_trades(ticker string, date time.Time) {
	ctx := context.Background()
	params := models.ListTradesParams{
		Ticker: ticker,
	}.WithLimit(50000).WithOrder(models.Asc).WithDay(date.Year(), date.Month(), date.Day())
	trades := e.polygon.ListTrades(ctx, params)
	sql := fmt.Sprintf("INSERT INTO %s.trades", e.viper.GetString("database"))
	batch, err := e.db.PrepareBatch(ctx, sql)
	if err != nil {
		panic(err)
	}
	for trades.Next() {
		t := trades.Item()
		err := batch.Append(
			uint64(t.SequenceNumber),
			e.tapes[int(t.Tape)],
			id_to_u64(t.ID),
			ticker,
			time.Time(t.SipTimestamp),
			parse_time(time.Time(t.ParticipantTimestamp)),
			parse_time(time.Time(t.TrfTimestamp)),
			t.Price,
			uint32(t.Size),
			convert_to_u8(t.Conditions),
			uint8(t.Correction),
			e.participants[int(t.Exchange)],
			e.participants[int(t.TrfID)],
		)
		if err != nil {
			panic(err)
		}
		atomic.AddUint64(&tradeCount, 1)
	}
	if trades.Err() != nil {
		panic(trades.Err())
	}
	err = batch.Send()
	if err != nil {
		panic(err)
	}
}

func (e *IngestCmd) worker(c chan string, wg *sync.WaitGroup, date time.Time) {
	for ticker := range c {
		e.download_and_flush_trades(ticker, date)
	}
	wg.Done()
}

func (e *IngestCmd) download_day(date time.Time) error {
	ticker_chan := make(chan string)
	wg := &sync.WaitGroup{}

	params := models.GetGroupedDailyAggsParams{
		Locale: models.MarketLocale("us"),
		MarketType: models.MarketType("stocks"),
		Date: models.Date(date),
	}.WithAdjusted(false)
	tickers, err := e.polygon.AggsClient.GetGroupedDailyAggs(context.Background(), params)
	if err != nil {
		return err
	}
	if tickers.ResultsCount == 0 {
		fmt.Printf("%s no data\n", date.Format("2006-01-02"))
		return nil
	}
	for i := 0; i < 1; i++ {
		wg.Add(1)
		go e.worker(ticker_chan, wg, date)
	}

	for _, ticker := range tickers.Results {
		ticker_chan <- ticker.Ticker
	}
	close(ticker_chan)
	wg.Wait()

	return nil
}

func Date(year, month, day int) time.Time {
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
}

func (e *IngestCmd) ingestTrades() error {
	e.tapes = map[int]string{}
	for key, val := range lib.Tapes {
		e.tapes[val] = key
	}
	e.participants = map[int]string{}
	for key, val := range lib.Participants {
		e.participants[val] = key
	}
	from, err := time.Parse(dateFormat, e.viper.GetString("from"))
	if err != nil {
		return err
	}
	to, err := time.Parse(dateFormat, e.viper.GetString("to"))
	if err != nil {
		return err
	}

	for d := to; d.Before(from) == false; d = d.AddDate(0, 0, -1) {
		tradeCount = 0
		begin := time.Now()
		if err := e.download_day(time.Time(d)); err != nil {
			return err
		}
		elapsed := time.Since(begin)
		fmt.Printf("%s %d trades took %s\n", d.Format("2006-01-02"), tradeCount, elapsed)
	}

	return nil
}
