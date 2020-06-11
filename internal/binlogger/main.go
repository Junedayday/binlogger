package main

import (
	"context"
	"flag"
	"time"

	"github.com/golang/glog"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/binlog"

	"github/Junedayday/binlogger/pbmysql"
	"github/Junedayday/binlogger/stream"
)

var (
	defaultTimeout = 2 * time.Minute
	cfg            = &mysql.ConnParams{
		Host:   "192.168.33.214",
		Port:   3306,
		Uname:  "dba",
		Pass:   "abc#123",
		DbName: "test",
	}
)

// vitess version list : https://proxy.golang.org/github.com/prometheus/client_golang/@v/list
func main() {
	flag.Parse()

	var err error
	var streamer stream.Steamer
	streamer, err = stream.NewEngine(cfg)
	if err != nil {
		glog.Fatal(err)
	}

	glog.Info("binlog start")
	var sc *binlog.SlaveConnection
	sc, err = binlog.NewSlaveConnection(cfg)
	if err != nil {
		glog.Fatal(err)
	}
	defer sc.Close()
	glog.Info("slave start")

	var pos mysql.Position
	var eCh <-chan mysql.BinlogEvent
	eCh, err = sc.StartBinlogDumpFromBinlogBeforeTimestamp(context.Background(), time.Now().Add(-2*time.Minute).Unix())
	if err != nil {
		glog.Errorf("start from history error %v, try start from current", err)
		pos, eCh, err = sc.StartBinlogDumpFromCurrent(context.Background())
		if err != nil {
			glog.Fatal(err)
		}
		glog.Infof("read binlog from %s\n", pos.String())
	}

	t := time.NewTimer(defaultTimeout)
	for {
		select {
		case <-t.C:
			glog.Fatal("time out")
		case e, ok := <-eCh:
			if !ok {
				glog.Fatal("channel close, stop program")
			}
			var msg *pbmysql.Event
			msg, err = streamer.ParseEvent(e)
			if err != nil {
				glog.Errorf("parseEvent %v", err)
				continue
			}
			t.Reset(defaultTimeout)
			if msg == nil || msg.Et == pbmysql.EventType_UnknownEvent {
				continue
			}
			glog.Info(msg)
		}
	}
}
