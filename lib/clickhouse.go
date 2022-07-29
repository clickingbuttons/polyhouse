package lib

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/spf13/viper"
)

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
		Debug: v.GetBool("verbose"),
	})
}
