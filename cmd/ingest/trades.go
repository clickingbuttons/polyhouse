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

func (e *IngestCmd) download_and_flush_trades(ticker string, date time.Time) {
	params := models.ListTradesParams{
		Ticker: ticker,
	}.WithLimit(50000).WithOrder(models.Asc).WithDay(date.Year(), date.Month(), date.Day())
	trades := e.polygon.ListTrades(e.ctx, params)
	sql := fmt.Sprintf("INSERT INTO %s.trades", e.viper.GetString("database"))
	batch, err := e.db.PrepareBatch(e.ctx, sql)
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

func (e *IngestCmd) worker_trades(c chan string, wg *sync.WaitGroup, date time.Time) {
	for ticker := range c {
		e.download_and_flush_trades(ticker, date)
	}
	wg.Done()
}

func (e *IngestCmd) download_day_trades(date time.Time, tickers []string) error {
	ticker_chan := make(chan string)
	wg := &sync.WaitGroup{}

	for i := 0; i < e.viper.GetInt("max-open-conns"); i++ {
		wg.Add(1)
		go e.worker_trades(ticker_chan, wg, date)
	}

	for _, ticker := range tickers {
		ticker_chan <- ticker
	}
	close(ticker_chan)
	wg.Wait()

	return nil
}
