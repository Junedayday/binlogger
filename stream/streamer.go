package stream

import (
	"github/Junedayday/binlogger/pbmysql"

	"vitess.io/vitess/go/mysql"
)

type Steamer interface {
	Reload() error
	ParseEvent(e mysql.BinlogEvent) (msg *pbmysql.Event, err error)
}
