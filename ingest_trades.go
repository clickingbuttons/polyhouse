package main

import (
	"strconv"
	"sync/atomic"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"

	polygon "github.com/polygon-io/client-go/rest"
	"github.com/polygon-io/client-go/rest/models"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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

func download_and_flush_trades(ticker string, date time.Time, client *polygon.Client, conn driver.Conn) {
	ctx := context.Background()
	params := models.ListTradesParams{
		Ticker: ticker,
	}.WithLimit(50000).WithOrder(models.Asc).WithDay(date.Year(), date.Month(), date.Day())
	trades := client.ListTrades(ctx, params)
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO us_equities.trades")
	if err != nil {
		panic(err)
	}
	for trades.Next() {
		t := trades.Item()
		err := batch.Append(
			uint64(t.SequenceNumber),
			uint8(t.Tape),
			id_to_u64(t.ID),
			ticker,
			time.Time(t.SipTimestamp),
			parse_time(time.Time(t.ParticipantTimestamp)),
			parse_time(time.Time(t.TrfTimestamp)),
			t.Price,
			uint32(t.Size),
			convert_to_u8(t.Conditions),
			uint8(t.Correction),
			uint8(t.Exchange),
			uint8(t.TrfID),
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

func worker(c chan string, wg *sync.WaitGroup, client *polygon.Client, date time.Time, conn driver.Conn) {
	for ticker := range c {
		download_and_flush_trades(ticker, date, client, conn)
	}
	wg.Done()
}

func download_day(client *polygon.Client, conn driver.Conn, date time.Time) {
	ticker_chan := make(chan string)
	wg := &sync.WaitGroup{}

	params := models.GetGroupedDailyAggsParams{
		Locale: models.MarketLocale("us"),
		MarketType: models.MarketType("stocks"),
		Date: models.Date(date),
	}.WithAdjusted(false)
	tickers, err := client.AggsClient.GetGroupedDailyAggs(context.Background(), params)
	if err != nil {
		panic(err)
	}
	if tickers.ResultsCount == 0 {
		fmt.Printf("%s no data\n", date.Format("2006-01-02"))
		return
	}
	for i := 0; i < 150; i++ {
		wg.Add(1)
		go worker(ticker_chan, wg, client, date, conn)
	}

	for _, ticker := range tickers.Results {
		ticker_chan <- ticker.Ticker
	}
	close(ticker_chan)
	wg.Wait()
}

func Date(year, month, day int) time.Time {
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
}

func main() {
	client := polygon.New(os.Getenv("POLY_API_KEY"))
	click, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		DialTimeout:  10 * time.Second,
		MaxOpenConns: 99,
		MaxIdleConns: 5,
		// Debug: true,
	})
	if err != nil {
		panic(err)
	}

	start := Date(2004, 1, 1)
	end := time.Now().AddDate(0, 0, -2)

	for d := end; d.Before(start) == false; d = d.AddDate(0, 0, -1) {
		tradeCount = 0
		begin := time.Now()
		download_day(client, click, time.Time(d))
		elapsed := time.Since(begin)
		fmt.Printf("%s %d trades took %s\n", d.Format("2006-01-02"), tradeCount, elapsed)
	}
}
