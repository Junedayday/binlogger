package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github/Junedayday/binlogger/pbmysql"
	"github/Junedayday/binlogger/stream"

	"github.com/golang/glog"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/binlog"
)

// vitess version list : https://proxy.golang.org/github.com/prometheus/client_golang/@v/list
func main() {
	var yamlFile string
	flag.StringVar(&yamlFile, "y", "/app/binlogsyncer.yaml", "include the yaml file")
	flag.Parse()
	defer glog.Flush()
	glog.Info("binlogsyncer start")

	var err error
	var cfg *binlogsyncerCfg
	if cfg, err = readCfg(yamlFile); err != nil {
		glog.Fatalf("read config file %s error %v", yamlFile, err)
	}
	cp := formatConn(cfg.Mysqlsrc)

	var streamer stream.Steamer
	streamer, err = stream.NewEngines([]*mysql.ConnParams{cp})
	if err != nil {
		glog.Fatal(err)
	}

	if cfg.Mysqlsrc.Id < 0 || cfg.Mysqlsrc.Id > 5 {
		glog.Fatalf("mysql id must >=1 and <= 5,but get %d", cfg.Mysqlsrc.Id)
	}

	/*
		这部分逻辑比较绕，主要是 创建n个slave=>关闭前n-1个slave=>用最后一个slave解析binlog
		这么做的主要原因是:
		binlog.NewSlaveConnection中的slaveId是私有的
		两个程序的slaveId相同时会报错
		所以临时取巧的办法是生成多个slave，slaveId会增加，然后关掉不需要的即可
	*/
	var scs = make([]*binlog.SlaveConnection, cfg.Mysqlsrc.Id)
	for i := 0; i < cfg.Mysqlsrc.Id; i++ {
		// 初始化即便slaveId重复也不会报错
		if scs[i], err = binlog.NewSlaveConnection(cp); err != nil {
			glog.Fatal(err)
		}
	}

	for i := 0; i < cfg.Mysqlsrc.Id-1; i++ {
		scs[i].Close()
	}
	var sc = scs[cfg.Mysqlsrc.Id-1]
	defer sc.Close()

	glog.Info("slave start")

	cpTgt := formatConn(cfg.Mysqltgt)
	var dbSync *stream.DbSyncer
	if dbSync, err = stream.NewSyncer(cpTgt); err != nil {
		glog.Fatalf("NewSyncer error %v", err)
	}
	glog.Info("connect to target mysql")

	var pos mysql.Position
	var eCh <-chan mysql.BinlogEvent
	eCh, err = sc.StartBinlogDumpFromBinlogBeforeTimestamp(context.Background(), time.Now().Add(-time.Duration(cfg.Mysqlsrc.Timeout)*time.Minute).Unix())
	if err != nil {
		glog.Errorf("start from history error %v, try start from current", err)
		pos, eCh, err = sc.StartBinlogDumpFromCurrent(context.Background())
		if err != nil {
			glog.Fatal(err)
		}
		glog.Infof("read binlog from %sn", pos.String())
	}

	var syncTab = make(map[string]struct{})
	for _, v := range cfg.Mysqlsrc.Tables {
		syncTab[v] = struct{}{}
	}

	t := time.NewTimer(time.Duration(cfg.Mysqlsrc.Timeout) * time.Minute)
	t2 := time.NewTimer(time.Duration(cfg.Mysqltgt.Timeout) * time.Minute)
	for {
		select {
		case <-t.C:
			glog.Fatal("read time out")
		case <-t2.C:
			glog.Fatal("sync time out")
		case e, ok := <-eCh:
			if !ok {
				glog.Fatal("channel close, stop program")
			}
			var msgs []*pbmysql.Event
			msgs, err = streamer.ParseEvent(e)
			if err != nil {
				glog.Errorf("parseEvent %v", err)
				continue
			}
			t.Reset(time.Duration(cfg.Mysqlsrc.Timeout) * time.Minute)
			if msgs == nil {
				continue
			}
			for _, msg := range msgs {
				if cfg.Mysqlsrc.Nodelete && msg.Et == pbmysql.EventType_DeleteEvent {
					continue
				} else if _, ok := syncTab[msg.Table]; !ok {
					continue
				}
				sql := formatSQL(cfg.Mysqltgt.Schema, msg)
				if sql == "" {
					continue
				}
				if err = dbSync.Exec(sql); err != nil {
					glog.Errorf("Exec SQL %s error %v", sql, err)
					continue
				}
			}
			t2.Reset(time.Duration(cfg.Mysqltgt.Timeout) * time.Minute)
		}
	}
}

func formatSQL(targetSchema string, msg *pbmysql.Event) string {
	if msg.Et == pbmysql.EventType_InsertEvent {
		return formatInsetSQL(targetSchema, msg)
	} else if msg.Et == pbmysql.EventType_UpdateEvent {
		return formatUpdateSQL(targetSchema, msg)
	}
	return ""
}

func formatInsetSQL(targetSchema string, pbmsg *pbmysql.Event) (originSQL string) {
	var columns, values = make([]string, len(pbmsg.Columns)), make([]string, len(pbmsg.Columns))
	for i, v := range pbmsg.Columns {
		columns[i] = "`" + v.Name + "`"
		if v.ValueNull {
			values[i] = "NULL"
		} else {
			values[i] = "'" + strings.ReplaceAll(string(v.Value), `'`, `\'`) + "'"
		}
	}
	originSQL = fmt.Sprintf("insert into `%s`.`%s` (%s) values (%s)", targetSchema, pbmsg.Table, strings.Join(columns, ","), strings.Join(values, ","))
	return
}

func formatUpdateSQL(targetSchema string, pbmsg *pbmysql.Event) (originSQL string) {
	originSQL = fmt.Sprintf("update `%s`.`%s` set ", targetSchema, pbmsg.Table)

	var setColumns = make([]string, len(pbmsg.Columns))
	for i, v := range pbmsg.Columns {
		if v.ValueNull {
			setColumns[i] = fmt.Sprintf("`%s` = NULL", v.Name)
		} else {
			setColumns[i] = fmt.Sprintf("`%s` = '%s'", v.Name, strings.ReplaceAll(string(v.Value), `'`, `\'`))
		}
	}
	originSQL += strings.Join(setColumns, ",")

	var wheres []string
	if len(pbmsg.PkColumns) > 0 {
		for _, v := range pbmsg.PkColumns {
			if pbmsg.Columns[v].BeforeNull {
				wheres = append(wheres, fmt.Sprintf("`%s` = NULL", pbmsg.Columns[v].Name))
			} else {
				wheres = append(wheres, fmt.Sprintf("`%s` = '%s'", pbmsg.Columns[v].Name, strings.ReplaceAll(string(pbmsg.Columns[v].Before), `'`, `\'`)))
			}

		}
	} else {
		for _, v := range pbmsg.Columns {
			if v.BeforeNull {
				wheres = append(wheres, fmt.Sprintf("`%s` = NULL", v.Name))
			} else {
				wheres = append(wheres, fmt.Sprintf("`%s` = '%s'", v.Name, strings.ReplaceAll(string(v.Before), `'`, `\'`)))
			}
		}
	}

	originSQL = originSQL + " where " + strings.Join(wheres, " and ")
	return
}
