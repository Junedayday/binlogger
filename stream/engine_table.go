package stream

import (
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
)

func (ei *Engine) addTable(id uint64, database, tableName string) {
	if _, ok := ei.matcher[id]; ok {
		return
	}
	ei.matcher[id] = ei.engines[database].GetTable(sqlparser.NewTableIdent(tableName))
}

func (ei *Engine) setTableMap(id uint64, tm *mysql.TableMap) {
	ei.tm[id] = tm
}

func (ei *Engine) getTable(id uint64) *schema.Table {
	return ei.matcher[id]
}

func (ei *Engine) getFormat() mysql.BinlogFormat {
	return ei.f
}

func (ei *Engine) setFormat(f mysql.BinlogFormat) {
	ei.f = f
}
