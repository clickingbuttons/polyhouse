package lib

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/spf13/viper"
)

// Can't contain commas because clickhouse-go can't parse them
var Tapes = map[string]int{
	"A-NYSE": 1,
	"B-ARCA": 2,
	"C-NASD": 3,
}
var Participants = map[string]int{
	"None":                               0,
	"NYSE American LLC":                  1,
	"Nasdaq OMX BX Inc.":                 2,
	"NYSE National Inc.":                 3,
	"FINRA Alternative Display Facility": 4,
	"Unlisted Trading Privileges":        5,
	"International Securities Exchange LLC - Stocks": 6,
	"Cboe EDGA":                        7,
	"Cboe EDGX":                        8,
	"NYSE Chicago Inc.":                9,
	"New York Stock Exchange":          10,
	"NYSE Arca Inc.":                   11,
	"Nasdaq":                           12,
	"Consolidated Tape Association":    13,
	"Long-Term Stock Exchange":         14,
	"Investors Exchange":               15,
	"Cboe Stock Exchange":              16,
	"Nasdaq Philadelphia Exchange LLC": 17,
	"Cboe BYX":                         18,
	"Cboe BZX":                         19,
	"MIAX Pearl":                       20,
	"Members Exchange":                 21,
	"OTC Equity Security":              62,
}

func MakeClickhouseClient(v *viper.Viper) (driver.Conn, error) {
	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{v.GetString("address")},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: v.GetString("username"),
			Password: v.GetString("password"),
		},
		DialTimeout:  10 * time.Second,
		MaxOpenConns: v.GetInt("max-open-conns"),
		MaxIdleConns: v.GetInt("max-idle-conns"),
		Debug:        v.GetBool("verbose"),
	})
}
