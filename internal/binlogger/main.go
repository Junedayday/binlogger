package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
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

	go func() {
		for {
			glog.Flush()
			time.Sleep(time.Second)
		}
	}()

	var err error
	var cfg *binLoggerCfg
	if cfg, err = readCfg(yamlFile); err != nil {
		glog.Fatalf("read config file %s error %v", yamlFile, err)
	}
	cps := formatConn(cfg.Mysql)

	var kp *kafka.Producer
	var ips []string
	for _, v := range cfg.Kafka.IP {
		ips = append(ips, fmt.Sprintf("%s:%d", v, cfg.Kafka.Port))
	}
	kp, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": strings.Join(ips, ",")})
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

	if cfg.Mysql.Id < 0 || cfg.Mysql.Id > 5 {
		glog.Fatalf("mysql id must >=1 and <= 5,but get %d", cfg.Mysql.Id)
	}

	/*
		这部分逻辑比较绕，主要是 创建n个slave=>关闭前n-1个slave=>用最后一个slave解析binlog
		这么做的主要原因是:
		binlog.NewSlaveConnection中的slaveId是私有的
		两个程序的slaveId相同时会报错
		所以临时取巧的办法是生成多个slave，slaveId会增加，然后关掉不需要的即可
	*/
	var scs = make([]*binlog.SlaveConnection, cfg.Mysql.Id)
	for i := 0; i < cfg.Mysql.Id; i++ {
		// 初始化即便slaveId重复也不会报错
		if scs[i], err = binlog.NewSlaveConnection(cps[0]); err != nil {
			glog.Fatal(err)
		}
	}

	for i := 0; i < cfg.Mysql.Id-1; i++ {
		scs[i].Close()
	}
	var sc = scs[cfg.Mysql.Id-1]
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
			var msgs []*pbmysql.Event
			msgs, err = streamer.ParseEvent(e)
			if err != nil {
				glog.Errorf("parseEvent %v", err)
				continue
			}
			t.Reset(time.Duration(cfg.Mysql.Timeout) * time.Minute)
			if msgs == nil {
				continue
			}
			glog.Infof("New Event count %d", len(msgs))
			for _, msg := range msgs {
				b, _ := proto.Marshal(msg)
				err = kp.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &cfg.Kafka.Topic, Partition: kafka.PartitionAny},
					Value:          b,
				}, nil)
				if err != nil {
					glog.Errorf("Produce error: %v", err)
				}
			}
		}
	}
}
