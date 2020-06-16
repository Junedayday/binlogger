package stream

import (
	"fmt"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"

	"github/Junedayday/binlogger/pbmysql"
)

// must be insert, update or delete
func (ei *Engine) getColumns(e mysql.BinlogEvent) (msg *pbmysql.Event, err error) {
	tID := e.TableID(ei.getFormat())

	if ei.tm[tID] == nil {
		return
	}
	var rows mysql.Rows
	if rows, err = e.Rows(ei.getFormat(), ei.tm[tID]); err != nil {
		return
	}

	msg = new(pbmysql.Event)
	msg.NanoTimestamp = time.Now().UnixNano()
	msg.Schema = ei.tm[tID].Database
	msg.Table = ei.tm[tID].Name

	if e.IsWriteRows() {
		err = ei.getInsertColumns(e, rows, tID, msg)
	} else if e.IsUpdateRows() {
		err = ei.getUpdateColumns(e, rows, tID, msg)
	} else if e.IsDeleteRows() {
		err = ei.getDeleteColumns(e, rows, tID, msg)
	} else {
		err = fmt.Errorf("unsupport type")
	}

	return
}

func (ei *Engine) getInsertColumns(e mysql.BinlogEvent, rows mysql.Rows, tID uint64, msg *pbmysql.Event) (err error) {
	msg.Et = pbmysql.EventType_InsertEvent
	msg.Columns = make([]*pbmysql.ColumnValue, rows.DataColumns.Count())
	for _, v := range ei.getTable(e.TableID(ei.getFormat())).PKColumns {
		msg.PkColumns = append(msg.PkColumns, int32(v))
	}

	var pos, valueIndex = 0, 0
	for i := 0; i < rows.DataColumns.Count(); i++ {
		msg.Columns[i] = new(pbmysql.ColumnValue)
		if !rows.DataColumns.Bit(i) {
			continue
		}

		msg.Columns[i].Name = ei.getTable(e.TableID(ei.getFormat())).Columns[i].Name.String()
		var l int
		msg.Columns[i].Value, l, err = ei.getValue(rows, pos, valueIndex, i, tID)
		if err != nil {
			return
		}
		pos += l
		valueIndex++
	}
	return
}

func (ei *Engine) getUpdateColumns(e mysql.BinlogEvent, rows mysql.Rows, tID uint64, msg *pbmysql.Event) (err error) {
	msg.Et = pbmysql.EventType_UpdateEvent
	msg.Columns = make([]*pbmysql.ColumnValue, rows.DataColumns.Count())
	for _, v := range ei.getTable(e.TableID(ei.getFormat())).PKColumns {
		msg.PkColumns = append(msg.PkColumns, int32(v))
	}

	var pos, valueIndex = 0, 0
	for i := 0; i < rows.DataColumns.Count(); i++ {
		msg.Columns[i] = new(pbmysql.ColumnValue)
		msg.Columns[i].Name = ei.getTable(e.TableID(ei.getFormat())).Columns[i].Name.String()
		if !rows.DataColumns.Bit(i) {
			continue
		}

		var l int
		msg.Columns[i].Value, l, err = ei.getValue(rows, pos, valueIndex, i, tID)
		if err != nil {
			return
		}
		pos += l
		valueIndex++
	}

	pos, valueIndex = 0, 0
	for i := 0; i < rows.DataColumns.Count(); i++ {
		if !rows.IdentifyColumns.Bit(i) {
			continue
		}

		var l int
		msg.Columns[i].Before, l, err = ei.getBefore(rows, pos, valueIndex, i, tID)
		if err != nil {
			return
		}
		pos += l
		valueIndex++
	}

	return
}

func (ei *Engine) getDeleteColumns(e mysql.BinlogEvent, rows mysql.Rows, tID uint64, msg *pbmysql.Event) (err error) {
	msg.Et = pbmysql.EventType_DeleteEvent
	msg.Columns = make([]*pbmysql.ColumnValue, rows.IdentifyColumns.Count())
	for _, v := range ei.getTable(e.TableID(ei.getFormat())).PKColumns {
		msg.PkColumns = append(msg.PkColumns, int32(v))
	}

	var pos, valueIndex = 0, 0
	for i := 0; i < rows.IdentifyColumns.Count(); i++ {
		msg.Columns[i] = new(pbmysql.ColumnValue)
		msg.Columns[i].Name = ei.getTable(e.TableID(ei.getFormat())).Columns[i].Name.String()
		if !rows.IdentifyColumns.Bit(i) {
			continue
		}

		var l int
		msg.Columns[i].Before, l, err = ei.getBefore(rows, pos, valueIndex, i, tID)
		if err != nil {
			return
		}
		pos += l
		valueIndex++
	}
	return
}

func (ei *Engine) getValue(rows mysql.Rows, pos, valueIndex, i int, tID uint64) (data []byte, l int, err error) {
	var value sqltypes.Value
	if rows.Rows[0].NullColumns.Bit(valueIndex) {
		return
	} else {
		value, l, err = mysql.CellValue(rows.Rows[0].Data, pos, ei.tm[tID].Types[i], ei.tm[tID].Metadata[i], ei.getTable(tID).Columns[i].Type)
		if err != nil {
			return
		}
		data = value.ToBytes()
	}
	return
}

func (ei *Engine) getBefore(rows mysql.Rows, pos, valueIndex, i int, tID uint64) (data []byte, l int, err error) {
	var value sqltypes.Value
	if rows.Rows[0].NullIdentifyColumns.Bit(valueIndex) {
		return
	} else {
		value, l, err = mysql.CellValue(rows.Rows[0].Identify, pos, ei.tm[tID].Types[i], ei.tm[tID].Metadata[i], ei.getTable(tID).Columns[i].Type)
		if err != nil {
			return
		}
		data = value.ToBytes()
	}
	return
}
