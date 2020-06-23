package stream

import (
	"context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type DbSyncer struct {
	ts *tabletserver.TabletServer
}

func NewSyncer(cp *mysql.ConnParams) (ds *DbSyncer, err error) {
	ds = new(DbSyncer)
	tabletCfg := tabletenv.DefaultQsConfig
	tabletCfg.EnableAutoCommit = true
	ds.ts = tabletserver.NewTabletServerWithNilTopoServer(tabletCfg)
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	err = ds.ts.StartService(target, dbconfigs.NewTestDBConfigs(*cp, *cp, cp.DbName))
	return
}

func (ds *DbSyncer) Exec(sql string) (err error) {
	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	_, err = ds.ts.QueryService().Execute(context.Background(), &target, sql, nil, 0, nil)
	return
}
