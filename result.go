package gohive

import (
	"database/sql/driver"
	"errors"
	"io"
	"reflect"

	"github.com/jellybean4/gohive/hive_service/rpc"
	log "github.com/sirupsen/logrus"
)

// HiveRows is an iterator over an executed query's results.
type HiveRows struct {
	stmt          *HiveStatement
	client        *rpc.TCLIServiceClient
	sessionHandle *rpc.TSessionHandle
	stmtHandle    *rpc.TOperationHandle
	columns       []*rpc.TColumnDesc
	fetchSize     int
	buffer        []*rpc.TRow
	hasMoreRows   bool
	currentIdx    int
}

func (r *HiveRows) init(fetchSize int) error {
	r.fetchSize = fetchSize
	r.hasMoreRows = true
	r.currentIdx = 0

	req := &rpc.TGetResultSetMetadataReq{
		OperationHandle: r.stmtHandle,
	}
	if resp, err := r.client.GetResultSetMetadata(req); err != nil {
		log.WithFields(log.Fields{
			"reason": err,
		}).Error("Hive: retrieve result metadata failed")
		return err
	} else if err := VerifySuccess(resp.GetStatus(), true); err != nil {
		return err
	} else {
		r.columns = resp.GetSchema().GetColumns()
		return nil
	}
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *HiveRows) Columns() []string {
	result := make([]string, len(r.columns))
	for _, c := range r.columns {
		result = append(result, c.GetColumnName())
	}
	return result
}

// Close closes the rows iterator.
func (r *HiveRows) Close() error {
	return r.stmt.closeOperation(r.stmtHandle)
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
func (r *HiveRows) Next(dest []driver.Value) error {
	if r.currentIdx >= len(r.buffer) {
		if err := r.nextPage(); err != nil {
			return err
		}
		r.currentIdx = 0
	}

	values := r.buffer[r.currentIdx].GetColVals()
	r.currentIdx++
	for _, v := range values {
		dest = append(dest, r.reflectValue(v))
	}
	return nil
}

func (r *HiveRows) reflectValue(value *rpc.TColumnValue) interface{} {
	switch {
	case value.IsSetBoolVal():
		return value.BoolVal.GetValue()
	case value.IsSetByteVal():
		return value.ByteVal.GetValue()
	case value.IsSetDoubleVal():
		return value.DoubleVal.GetValue()
	case value.IsSetI16Val():
		return value.I16Val.GetValue()
	case value.IsSetI32Val():
		return value.I32Val.GetValue()
	case value.IsSetI64Val():
		return value.I64Val.GetValue()
	case value.IsSetStringVal():
		return value.StringVal.GetValue()
	}
	return nil
}

func (r *HiveRows) nextPage() error {
	if !r.hasMoreRows {
		log.Info("Hive: No more rows")
		return io.EOF
	}
	req := &rpc.TFetchResultsReq{
		OperationHandle: r.stmtHandle,
		MaxRows:         int64(r.fetchSize),
	}
	if resp, err := r.readResult(req); err != nil {
		return err
	} else {
		r.hasMoreRows = resp.GetHasMoreRows()
		r.buffer = resp.GetResults().GetRows()
		return nil
	}
}

// GetQueryLog returns the logs generated when query running
func (r *HiveRows) GetQueryLog() ([]string, error) {
	req := &rpc.TFetchResultsReq{
		OperationHandle: r.stmtHandle,
		Orientation:     rpc.TFetchOrientation_FETCH_NEXT,
		MaxRows:         int64(r.fetchSize),
		FetchType:       1,
	}
	if resp, err := r.readResult(req); err != nil {
		return nil, err
	} else {
		result := make([]string, 10)
		for _, r := range resp.GetResults().GetRows() {
			result = append(result, r.String())
		}
		return result, nil
	}
}

func (r *HiveRows) readResult(req *rpc.TFetchResultsReq) (*rpc.TFetchResultsResp, error) {
	if resp, err := r.client.FetchResults(req); err != nil {
		log.WithFields(log.Fields{
			"reason": err,
		}).Error("Hive: Get query log failed")
		return nil, err
	} else if err := VerifySuccess(resp.GetStatus(), true); err != nil {
		log.Error("Hive: Get query log response error")
		return nil, err
	} else {
		return resp, nil
	}
}

// ColumnTypeScanType may be implemented by Rows. It should return
// the value type that can be used to scan types into. For example, the database
// column type "bigint" this should return "reflect.TypeOf(int64(0))".
func (r *HiveRows) ColumnTypeScanType(index int) reflect.Type {
	if name := r.ColumnTypeDatabaseTypeName(index); len(name) == 0 {
		return reflect.TypeOf(new(interface{}))
	} else {
		return r.mapType(name)
	}
}

func (r *HiveRows) mapType(name string) reflect.Type {
	if t, ok := REFLECT_MAP[name]; !ok {
		log.WithFields(log.Fields{
			"type": name,
		}).Error("Hive: type name error")
		return reflect.TypeOf(new(interface{}))
	} else {
		return t
	}
}

// ColumnTypeDatabaseTypeName may be implemented by Rows. It should return the
// database system type name without the length. Type names should be uppercase.
// Examples of returned types: "VARCHAR", "NVARCHAR", "VARCHAR2", "CHAR", "TEXT",
// "DECIMAL", "SMALLINT", "INT", "BIGINT", "BOOL", "[]BIGINT", "JSONB", "XML",
// "TIMESTAMP".
func (r *HiveRows) ColumnTypeDatabaseTypeName(index int) string {
	if entry, err := r.columnPrimitiveEntry(index); err != nil {
		return ""
	} else if name, ok := rpc.TYPE_NAMES[entry.GetType()]; !ok {
		log.WithFields(log.Fields{
			"type": entry.GetType,
		}).Error("Hive: type code error")
		return ""
	} else {
		log.WithFields(log.Fields{
			"typeName": name,
		}).Debug("Hive: Get column database type success")
		return name
	}
}

func (r *HiveRows) columnPrimitiveEntry(index int) (*rpc.TPrimitiveTypeEntry, error) {
	if index >= len(r.columns) {
		log.WithFields(log.Fields{
			"index":  index,
			"length": len(r.columns),
		}).Error("Hive: Scan column database type with index out of bound")
		return nil, errors.New("index out of bound")
	} else if types := r.columns[index].GetTypeDesc().GetTypes(); len(types) <= 0 {
		log.WithFields(log.Fields{
			"index":  index,
			"column": r.columns[index].String(),
		}).Error("Hive: Column database types is empty")
		return nil, errors.New("database types not found")
	} else {
		return types[0].GetPrimitiveEntry(), nil
	}
}

// HiveResult is the result of a query execution.
type HiveResult struct {
	stmt          *HiveStatement
	client        *rpc.TCLIServiceClient
	sessionHandle *rpc.TSessionHandle
	stmtHandle    *rpc.TOperationHandle
	modifiedRows  int64
}

func (r *HiveResult) init() {
	r.modifiedRows = int64(r.stmtHandle.GetModifiedRowCount())
}

// LastInsertId returns the database's auto-generated ID
// after, for example, an INSERT into a table with primary
// key.
func (r *HiveResult) LastInsertId() (int64, error) {
	return -1, ERROR_NOT_SUPPORTED
}

// RowsAffected returns the number of rows affected by the
// query.
func (r *HiveResult) RowsAffected() (int64, error) {
	return r.modifiedRows, nil
}
