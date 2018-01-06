package gohive

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/jellybean4/gohive/hive_service/rpc"
	log "github.com/sirupsen/logrus"
)

// HiveStatement is a prepared statement. It is bound to a Conn and not
// used by multiple goroutines concurrently.
type HiveStatement struct {
	connection    *HiveConn
	rawQuery      string
	argsCount     int
	lock          *sync.Mutex
	parts         []string
	sessionHandle *rpc.TSessionHandle
}

type hiveExecContext struct {
	session   *rpc.TSessionHandle
	operation *rpc.TOperationHandle
}

// NewHiveStatement create a new driver.stmt with the given conn
func NewHiveStatement(conn *HiveConn, query string) (*HiveStatement, error) {
	parts := SplitWith(query, '?')
	stmt := &HiveStatement{
		connection:    conn,
		rawQuery:      query,
		argsCount:     len(parts),
		lock:          &sync.Mutex{},
		parts:         parts,
		sessionHandle: conn.SessionHandle,
	}
	return stmt, nil
}

// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use
// by any queries.
func (stmt *HiveStatement) Close() error {
	stmt.connection = nil
	stmt = nil
	return nil
}

// NumInput returns the number of placeholder parameters.
//
// If NumInput returns >= 0, the sql package will sanity check
// argument counts from callers and return errors to the caller
// before the statement's Exec or Query methods are called.
//
// NumInput may also return -1, if the driver doesn't know
// its number of placeholders. In that case, the sql package
// will not sanity check Exec or Query argument counts.
func (stmt *HiveStatement) NumInput() int {
	return stmt.argsCount
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// Deprecated: Drivers should implement StmtExecContext instead (or additionally).
func (stmt *HiveStatement) Exec(args []driver.Value) (driver.Result, error) {
	stmt.lock.Lock()
	ctx := &hiveExecContext{session: stmt.sessionHandle}
	stmt.lock.Unlock()

	defer func() {
		stmt.closeOperation(ctx.operation)
	}()

	if err := stmt.exec(ctx, args); err != nil {
		return nil, err
	} else {
		return stmt.buildHiveResult(ctx), nil
	}
}

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// ExecContext must honor the context timeout and return when it is canceled.
func (stmt *HiveStatement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	stmt.lock.Lock()
	hiveCtx := &hiveExecContext{session: stmt.sessionHandle}
	stmt.lock.Unlock()

	defer func() {
		stmt.closeOperation(hiveCtx.operation)
	}()

	recv := make(chan error)
	go func() {
		recv <- stmt.exec(hiveCtx, NamedValueToValue(args))
	}()

	select {
	case <-ctx.Done():
		log.WithFields(log.Fields{
			"query": stmt.rawQuery,
		}).Info("Hive: Execute canceled")
		return nil, errors.New("execute canceled")
	case err := <-recv:
		if err != nil {
			return nil, err
		}
		return stmt.buildHiveResult(hiveCtx), nil
	}
}

// Query executes a query that may return rows, such as a
// SELECT.
//
// Deprecated: Drivers should implement StmtQueryContext instead (or additionally).
func (stmt *HiveStatement) Query(args []driver.Value) (rows driver.Rows, _ error) {
	stmt.lock.Lock()
	ctx := &hiveExecContext{session: stmt.sessionHandle}
	stmt.lock.Unlock()

	defer func() {
		if rows == nil {
			stmt.closeOperation(ctx.operation)
		}
	}()

	if err := stmt.exec(ctx, args); err != nil {
		return nil, err
	} else if !ctx.operation.HasResultSet {
		log.WithFields(log.Fields{
			"query": stmt.processQuery,
		}).Error("Hive: Query finished without result set")
		return nil, errors.New("query finished without result set")
	} else if r, err := stmt.buildHiveRows(ctx); err != nil {
		return nil, err
	} else {
		rows = r
		return rows, nil
	}
}

// QueryContext executes a query that may return rows, such as a
// SELECT.
//
// QueryContext must honor the context timeout and return when it is canceled.
func (stmt *HiveStatement) QueryContext(ctx context.Context, args []driver.NamedValue) (rows driver.Rows, _ error) {
	stmt.lock.Lock()
	hiveCtx := &hiveExecContext{session: stmt.sessionHandle}
	stmt.lock.Unlock()

	defer func() {
		if rows == nil {
			stmt.closeOperation(hiveCtx.operation)
		}
	}()

	recv := make(chan error)
	go func() {
		recv <- stmt.exec(hiveCtx, NamedValueToValue(args))
	}()

	select {
	case <-ctx.Done():
		log.WithFields(log.Fields{
			"query": stmt.rawQuery,
		}).Info("Hive: Query canceled")
		return nil, errors.New("execute canceled")
	case err := <-recv:
		if err != nil {
			return nil, err
		} else if !hiveCtx.operation.HasResultSet {
			log.WithFields(log.Fields{
				"query": stmt.processQuery,
			}).Error("Hive: Query finished without result set")
			return nil, errors.New("query finished without result set")
		} else if r, err := stmt.buildHiveRows(hiveCtx); err != nil {
			return nil, err
		} else {
			rows = r
			return rows, nil
		}
	}
}

// CheckNamedValue is called before passing arguments to the driver
// and is called in place of any ColumnConverter. CheckNamedValue must do type
// validation and conversion as appropriate for the driver.
func (stmt *HiveStatement) CheckNamedValue(value *driver.NamedValue) error {
	if value.Name != "" {
		return errors.New("named value not supported")
	} else if _, ok := value.Value.(string); !ok {
		return errors.New("only string type value supported")
	}
	return nil
}

func (stmt *HiveStatement) exec(ctx *hiveExecContext, args []driver.Value) error {
	if stmt.rawQuery == "" {
		return ERROR_PREPARED_QUERY_NOT_FOUND
	} else if processedQuery, err := stmt.processQuery(args); err != nil {
		return err
	} else if err := stmt.hiveExec(ctx, processedQuery); err != nil {
		return err
	} else if err := stmt.waitResponse(ctx.operation); err != nil {
		return err
	} else {
		return nil
	}
}

func (stmt *HiveStatement) hiveExec(ctx *hiveExecContext, query string) error {
	stmtReq := &rpc.TExecuteStatementReq{
		SessionHandle: ctx.session,
		Statement:     query,
		RunAsync:      stmt.connection.Config.RunAsync,
		QueryTimeout:  int64(stmt.connection.Config.ExecTimeout),
	}

	client := stmt.connection.Client
	if resp, err := client.ExecuteStatement(context.Background(), stmtReq); err != nil {
		log.WithFields(log.Fields{
			"reason": err,
			"query":  query,
		}).Error("Hive: Execute query failed")
		return err
	} else if err := VerifySuccess(resp.GetStatus(), true); err != nil {
		log.WithFields(log.Fields{
			"query": query,
		}).Error("Hive: Execute query failed")
		return err
	} else if handle := resp.GetOperationHandle(); handle == nil {
		log.WithFields(log.Fields{
			"query": query,
		}).Error("Hive: Execute query response handler null")
		return errors.New("execute query response handler null")
	} else {
		ctx.operation = handle
		return nil
	}
}

func (stmt *HiveStatement) waitResponse(handle *rpc.TOperationHandle) error {
	statusReq := &rpc.TGetOperationStatusReq{OperationHandle: handle}
	client := stmt.connection.Client
	for {
		if resp, err := client.GetOperationStatus(context.Background(), statusReq); err != nil {
			log.WithFields(log.Fields{
				"reason": err,
			}).Error("Hive: Query execute status failed")
			return err
		} else if err = VerifySuccess(resp.GetStatus(), true); err != nil {
			log.WithFields(log.Fields{
				"reason": err,
			}).Error("Hive: Query execute status failed")
			return err
		} else if complete, err := stmt.checkRespStatus(resp); err != nil {
			return err
		} else if complete {
			break
		}
	}
	return nil
}

func (stmt *HiveStatement) buildHiveRows(ctx *hiveExecContext) (*HiveRows, error) {
	rows := &HiveRows{
		stmt:          stmt,
		client:        stmt.connection.Client,
		sessionHandle: ctx.session,
		stmtHandle:    ctx.operation,
	}
	if err := rows.init(1000); err != nil {
		return nil, err
	} else {
		return rows, nil
	}
}

func (stmt *HiveStatement) buildHiveResult(ctx *hiveExecContext) *HiveResult {
	result := &HiveResult{
		stmt:          stmt,
		client:        stmt.connection.Client,
		sessionHandle: ctx.session,
		stmtHandle:    ctx.operation,
	}
	result.init()
	return result
}

func (stmt *HiveStatement) checkRespStatus(resp *rpc.TGetOperationStatusResp) (bool, error) {
	if resp.IsSetOperationState() {
		switch resp.GetOperationState() {
		case rpc.TOperationState_CLOSED_STATE, rpc.TOperationState_FINISHED_STATE:
			return true, nil
		case rpc.TOperationState_CANCELED_STATE:
			log.Info("Hive: Query canceled")
			return false, errors.New("query canceled")
		case rpc.TOperationState_ERROR_STATE:
			return false, fmt.Errorf("query failed with message: %s, state: %s, code: %d",
				resp.GetErrorMessage(), resp.GetSqlState(), resp.GetErrorCode())
		case rpc.TOperationState_UKNOWN_STATE:
			return false, errors.New("query state unknown")
		case rpc.TOperationState_INITIALIZED_STATE, rpc.TOperationState_PENDING_STATE, rpc.TOperationState_RUNNING_STATE:
			return false, nil
		}
	}
	return false, nil
}

func (stmt *HiveStatement) processQuery(args []driver.Value) (string, error) {
	query := stmt.rawQuery
	if stmt.argsCount != len(args) {
		log.WithFields(log.Fields{
			"query":     query,
			"argsCount": len(args),
			"expected":  stmt.NumInput(),
		}).Error("Hive: Args count not match placeholder")
		return "", errors.New("args count not match placeholder")
	}

	// quick return
	if stmt.argsCount == 0 {
		return query, nil
	}

	// make sure all value is string
	values := make([]string, len(args))
	for _, v := range args {
		strV, ok := v.(string)
		if !ok {
			return "", errors.New("Only string value is supported")
		}
		values = append(values, strV)
	}

	return strings.Join(stmt.parts, " "), nil
}

func (stmt *HiveStatement) closeOperation(operation *rpc.TOperationHandle) error {
	if operation != nil {
		req := &rpc.TCloseOperationReq{
			OperationHandle: operation,
		}
		_, err := stmt.connection.Client.CloseOperation(context.Background(), req)
		if err != nil {
			log.WithFields(log.Fields{
				"reason": err,
			}).Error("Hive: close operation failed")
			return err
		}
		return nil
	} else {
		log.Debug("Hive: close operation handler success")
		return nil
	}
}
