package main

import (
	"io/ioutil"

	"gopkg.in/yaml.v3"
	"vitess.io/vitess/go/mysql"
)

type mysqlCfg struct {
	User     string
	Password string
	IP       string
	Port     int
	Schemas  []string
	// time out for reading binlog: minutes
	Timeout int
	Id      int
}

type kafkaCfg struct {
	IP    []string
	Port  int
	Topic string
}

type binLoggerCfg struct {
	Mysql *mysqlCfg
	Kafka *kafkaCfg
}

func readCfg(yamlFile string) (*binLoggerCfg, error) {
	var cfg = new(binLoggerCfg)
	b, err := ioutil.ReadFile(yamlFile)
	if err != nil {
		return nil, err
	} else if err = yaml.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	return cfg, err
}

func formatConn(cfg *mysqlCfg) (cps []*mysql.ConnParams) {
	for _, v := range cfg.Schemas {
		cps = append(cps, &mysql.ConnParams{
			Host:   cfg.IP,
			Port:   cfg.Port,
			Uname:  cfg.User,
			Pass:   cfg.Password,
			DbName: v,
		})
	}
	return
}
