package ingest

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/polygon-io/client-go/rest/models"
)

var tradeCount uint64

type Trade struct {
	models.Trade
	Ticker string
}

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

func parse_time_unix(t time.Time) *int64 {
	var default_time time.Time
	if t == default_time {
		return nil
	}
	unix := int64(t.Unix())
	return &unix
}

func (e *IngestCmd) download_trades(ticker string, date time.Time, trades_chan chan Trade) {
	params := models.ListTradesParams{
		Ticker: ticker,
	}.WithLimit(50000).WithOrder(models.Asc).WithDay(date.Year(), date.Month(), date.Day())
	trades := e.polygon.ListTrades(e.ctx, params)
	for trades.Next() {
		t := trades.Item()
		trades_chan <- Trade{
			Trade:  t,
			Ticker: ticker,
		}
		atomic.AddUint64(&tradeCount, 1)
	}
	if trades.Err() != nil {
		panic(trades.Err())
	}
}

func (e *IngestCmd) flush_trades(trades_chan chan Trade) error {
	sql := fmt.Sprintf("INSERT INTO %s.trades", e.viper.GetString("database"))
	batch, err := e.db.PrepareBatch(e.ctx, sql)
	if err != nil {
		panic(err)
	}
	for t := range trades_chan {
		exchange, ok := e.participants[t.Exchange]
		if !ok {
			return fmt.Errorf("unknown exchange %d for %s", t.Exchange, t.Ticker)
		}
		trf, ok := e.participants[t.TrfID]
		if !ok {
			return fmt.Errorf("unknown trfID %d for %s", t.TrfID, t.Ticker)
		}
		err := batch.Append(
			uint64(t.SequenceNumber),
			e.tapes[int(t.Tape)],
			id_to_u64(t.ID),
			t.Ticker,
			time.Time(t.SipTimestamp),
			parse_time(time.Time(t.ParticipantTimestamp)),
			parse_time(time.Time(t.TrfTimestamp)),
			t.Price,
			uint32(t.Size),
			convert_to_u8(t.Conditions),
			uint8(t.Correction),
			exchange,
			trf,
		)
		if err != nil {
			return err
		}
	}
	err = batch.Send()
	if err != nil {
		panic(err)
	}
	return nil
}

func (e *IngestCmd) download_day_trades(date time.Time, tickers []string) error {
	ticker_chan := make(chan string)
	trades_chan := make(chan Trade, 200_000_000)
	wg := &sync.WaitGroup{}

	for i := 0; i < e.viper.GetInt("max-open-conns"); i++ {
		wg.Add(1)
		go func() {
			for ticker := range ticker_chan {
				e.download_trades(ticker, date, trades_chan)
			}
			wg.Done()
		}()
	}

	for _, ticker := range tickers {
		ticker_chan <- ticker
	}
	close(ticker_chan)
	wg.Wait()

	close(trades_chan)
	return e.flush_trades(trades_chan)
}
