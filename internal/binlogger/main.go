package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go-dev/kafka"
	"google.golang.org/protobuf/proto"

	"github/Junedayday/binlogger/pbmysql"
	"github/Junedayday/binlogger/stream"

	"github.com/golang/glog"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/binlog"
)

// vitess version list : https://proxy.golang.org/github.com/prometheus/client_golang/@v/list
func main() {
	var yamlFile string
	flag.StringVar(&yamlFile, "y", "/app/binlogger.yaml", "include the yaml file")
	flag.Parse()
	defer glog.Flush()
	glog.Info("binlogger start")

	var err error
	var cfg *binLoggerCfg
	if cfg, err = readCfg(yamlFile); err != nil {
		glog.Fatal("read config file %s error %v", yamlFile, err)
	}
	cps := formatConn(cfg.Mysql)

	var kp *kafka.Producer
	kp, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": fmt.Sprintf("%s:%d", cfg.Kafka.IP, cfg.Kafka.Port)})
	if err != nil {
		glog.Fatal(err)
	}
	defer kp.Close()

	go func() {
		for e := range kp.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					glog.Errorf("Delivery failed: %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	var streamer stream.Steamer
	streamer, err = stream.NewEngines(cps)
	if err != nil {
		glog.Fatal(err)
	}

	glog.Info("binlog start")
	var sc *binlog.SlaveConnection
	sc, err = binlog.NewSlaveConnection(cps[0])
	if err != nil {
		glog.Fatal(err)
	}
	defer sc.Close()
	glog.Info("slave start")

	var pos mysql.Position
	var eCh <-chan mysql.BinlogEvent
	eCh, err = sc.StartBinlogDumpFromBinlogBeforeTimestamp(context.Background(), time.Now().Add(-time.Duration(cfg.Mysql.Timeout)*time.Minute).Unix())
	if err != nil {
		glog.Errorf("start from history error %v, try start from current", err)
		pos, eCh, err = sc.StartBinlogDumpFromCurrent(context.Background())
		if err != nil {
			glog.Fatal(err)
		}
		glog.Infof("read binlog from %sn", pos.String())
	}

	t := time.NewTimer(time.Duration(cfg.Mysql.Timeout) * time.Minute)
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
			t.Reset(time.Duration(cfg.Mysql.Timeout) * time.Minute)
			if msg == nil || msg.Et == pbmysql.EventType_UnknownEvent {
				continue
			}
			b, _ := proto.Marshal(msg)
			err = kp.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &cfg.Kafka.Topic, Partition: kafka.PartitionAny},
				Value:          b,
			}, nil)
			if err != nil {
				glog.Errorf("Produce error: %v", err)
			}
			// glog.V(1).Info(msg)
		}
	}
}
