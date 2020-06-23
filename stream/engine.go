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
	engines map[string]*schema.Engine
	matcher map[uint64]*schema.Table
	tm      map[uint64]*mysql.TableMap
	f       mysql.BinlogFormat
}

func NewEngines(cps []*mysql.ConnParams) (Steamer, error) {
	ei := new(Engine)
	ei.engines = make(map[string]*schema.Engine)
	ei.matcher = make(map[uint64]*schema.Table)
	ei.tm = make(map[uint64]*mysql.TableMap)
	for _, v := range cps {
		if err := ei.init(v); err != nil {
			return nil, err
		}
	}
	return ei, nil
}

func (ei *Engine) init(cp *mysql.ConnParams) (err error) {
	tbServer := tabletserver.NewTabletServerWithNilTopoServer(tabletenv.DefaultQsConfig)
	ei.engines[cp.DbName] = schema.NewEngine(tbServer, tabletenv.TabletConfig{})
	cfg := dbconfigs.NewTestDBConfigs(*cp, *cp, cp.DbName)
	ei.engines[cp.DbName].InitDBConfig(cfg)
	err = ei.engines[cp.DbName].Open()
	if err != nil {
		return
	}
	return
}

func (ei *Engine) Reload() (err error) {
	for _, v := range ei.engines {
		if err = v.Reload(context.Background()); err != nil {
			return
		}
	}
	return
}

func (ei *Engine) ParseEvent(e mysql.BinlogEvent) (msgs []*pbmysql.Event, err error) {
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
				panic("reload error" + err.Error())
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
		} else if _, ok := ei.engines[tm.Database]; !ok {
			return
		}
		ei.addTable(tabId, tm.Database, tm.Name)
		ei.setTableMap(tabId, tm)
	}

	if e.IsWriteRows() || e.IsUpdateRows() || e.IsDeleteRows() {
		msgs, err = ei.getColumns(e)
	}
	return
}
