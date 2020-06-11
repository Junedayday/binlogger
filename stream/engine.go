package stream

import (
	"context"
	"fmt"
	"strings"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	"github/Junedayday/binlogger/pbmysql"
)

var ddlMap = map[string]struct{}{
	"create":   {},
	"alter":    {},
	"drop":     {},
	"truncate": {},
	"rename":   {},
	"set":      {},
}

type Engine struct {
	schemaName string
	engine     *schema.Engine
	matcher    map[uint64]*schema.Table
	tm         map[uint64]*mysql.TableMap
	f          mysql.BinlogFormat
}

func NewEngine(cp *mysql.ConnParams) (Steamer, error) {
	ei := new(Engine)
	err := ei.init(cp)
	return ei, err
}

func (ei *Engine) init(cp *mysql.ConnParams) (err error) {
	ei.schemaName = cp.DbName
	tbServer := tabletserver.NewTabletServerWithNilTopoServer(tabletenv.DefaultQsConfig)
	ei.engine = schema.NewEngine(tbServer, tabletenv.TabletConfig{})
	cfg := dbconfigs.NewTestDBConfigs(*cp, *cp, cp.DbName)
	ei.engine.InitDBConfig(cfg)
	err = ei.engine.Open()
	if err != nil {
		return
	}
	ei.matcher = make(map[uint64]*schema.Table)
	ei.tm = make(map[uint64]*mysql.TableMap)
	return
}

func (ei *Engine) Reload() (err error) {
	err = ei.engine.Reload(context.Background())
	return
}

func (ei *Engine) ParseEvent(e mysql.BinlogEvent) (msg *pbmysql.Event, err error) {
	if !e.IsValid() {
		err = fmt.Errorf("not valid")
		return
	}

	if e.IsFormatDescription() {
		var f mysql.BinlogFormat
		if f, err = e.Format(); err != nil {
			return
		}
		ei.setFormat(f)
	}

	if e, _, err = e.StripChecksum(ei.getFormat()); err != nil {
		return
	}

	// if ei.GetFormat().IsZero() {
	// 	return
	// }

	if e.IsQuery() {
		var q mysql.Query
		q, err = e.Query(ei.getFormat())
		if err != nil {
			return
		}
		var sql = q.SQL
		if i := strings.IndexByte(sql, byte(' ')); i >= 0 {
			sql = q.SQL[:i]
		}
		if _, ok := ddlMap[strings.ToLower(sql)]; ok {
			err = fmt.Errorf("get a ddl : \n%s", q.SQL)
			if err = ei.Reload(); err != nil {
				panic("reload error " + err.Error())
			}
			return
		}
	}

	var tm *mysql.TableMap
	if e.IsTableMap() {
		tabId := e.TableID(ei.getFormat())
		tm, err = e.TableMap(ei.getFormat())
		if err != nil {
			return
		} else if tm.Database != ei.schemaName {
			return
		}
		ei.addTable(tabId, tm.Name)
		ei.setTableMap(tabId, tm)
	}

	msg = new(pbmysql.Event)
	if e.IsWriteRows() || e.IsUpdateRows() || e.IsDeleteRows() {
		msg, err = ei.getColumns(e)
	} else {
		msg.Et = pbmysql.EventType_UnknownEvent
		return
	}
	return
}
