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
	Schema   string
	Tables   []string
	// time out for reading binlog: minutes
	Timeout  int
	Id       int
	Nodelete bool
}

type binlogsyncerCfg struct {
	Mysqlsrc *mysqlCfg
	Mysqltgt *mysqlCfg
}

func readCfg(yamlFile string) (*binlogsyncerCfg, error) {
	var cfg = new(binlogsyncerCfg)
	b, err := ioutil.ReadFile(yamlFile)
	if err != nil {
		return nil, err
	} else if err = yaml.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	return cfg, err
}

func formatConn(cfg *mysqlCfg) *mysql.ConnParams {
	return &mysql.ConnParams{
		Host:   cfg.IP,
		Port:   cfg.Port,
		Uname:  cfg.User,
		Pass:   cfg.Password,
		DbName: cfg.Schema,
	}
}
