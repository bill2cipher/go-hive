package gohive

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"

	"git.apache.org/thrift.git/lib/go/thrift"
	sasl "github.com/jellybean4/go-sasl"
	"github.com/jellybean4/gohive/hive_service/rpc"
	stransport "github.com/jellybean4/thrift-sasl"
	log "github.com/sirupsen/logrus"
)

const (
	// HiveAuthNoSasl indicate no sasl need here
	HiveAuthNoSasl = "nosasl"
	// HiveAuthPlain is the default sasl mechanism
	HiveAuthPlain = "plain"
	// HiveAuthKerberos indicate kerberos sasl mechanism need here
	HiveAuthKerberos = "kerberos"
)

// HiveConn implements driver.Conn
type HiveConn struct {
	Config          *Config
	Transport       thrift.TTransport
	Client          *rpc.TCLIServiceClient
	SessionHandle   *rpc.TSessionHandle
	OperationHandle *rpc.TOperationHandle
	lock            *sync.Mutex
}

// NewHiveConn build a new driver.Conn with the given dsn
func NewHiveConn(dsn string) (*HiveConn, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		log.WithFields(log.Fields{
			"reason": err,
			"dsn":    dsn,
		}).Error("parse dsn failed")
		return nil, err
	}

	conn := &HiveConn{
		Config: cfg,
		lock:   &sync.Mutex{},
	}

	if err := conn.init(); err != nil {
		return nil, err
	} else {
		return conn, nil
	}
}

func (conn *HiveConn) init() error {
	cfg := conn.Config
	sock, err := thrift.NewTSocketTimeout(cfg.Address(), cfg.DialTimeout)
	if err != nil {
		log.WithFields(log.Fields{
			"reason":  err,
			"address": cfg.Address(),
		}).Error("Hive: Create hive sock failed")
		return err
	}
	if err := sock.Open(); err != nil {
		log.WithFields(log.Fields{
			"reason":  err,
			"address": cfg.Address,
		}).Error("Hive: Open hive sock failed")
	} else if cfg.Auth == HiveAuthNoSasl {
		conn.Transport = sock
	} else if client, err := conn.buildSaslClient(); err != nil {
		return err
	} else if trans, err := stransport.NewTSaslTransport(sock, client); err != nil {
		conn.Transport = trans
	}

	protocol := thrift.NewTBinaryProtocolFactoryDefault()
	conn.Client = rpc.NewTCLIServiceClientFactory(conn.Transport, protocol)
	if err := conn.openSession(); err != nil {
		return err
	} else {
		log.Debug("Hive: Open connection success")
		return nil
	}
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (conn *HiveConn) Begin() (driver.Tx, error) {
	return nil, ERROR_NOT_SUPPORTED
}

// Prepare returns a prepared statement, bound to this connection.
func (conn *HiveConn) Prepare(query string) (driver.Stmt, error) {
	return NewHiveStatement(conn, query)
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
func (conn *HiveConn) Close() error {
	conn.lock.Lock()
	defer conn.lock.Unlock()

	errList := make([]error, 4)

	if err := conn.closeSession(); err != nil {
		errList = append(errList, err)
	}

	if err := conn.Transport.Close(); err != nil {
		log.WithFields(log.Fields{
			"reason": err,
		}).Error("Hive: close sock failed")
		errList = append(errList, err)
	}

	if len(errList) == 0 {
		return nil
	}
	return errList[0]
}

// Ping is an optional interface that may be implemented by a Conn.
//
// If a Conn does not implement Pinger, the sql package's DB.Ping and
// DB.PingContext will check if there is at least one Conn available.
//
// If Conn.Ping returns ErrBadConn, DB.Ping and DB.PingContext will remove
// the Conn from pool.
func (conn *HiveConn) Ping(ctx context.Context) error {
	if conn.Transport.IsOpen() {
		return driver.ErrBadConn
	}

	recv := make(chan error)
	go func() {
		recv <- conn.checkSession()
	}()

	select {
	case <-ctx.Done():
		return errors.New("ping canceled before return")
	case err := <-recv:
		return err
	}
}

// Exec is an optional interface that may be implemented by a Conn.
//
// If a Conn does not implement Execer, the sql package's DB.Exec will
// first prepare a query, execute the statement, and then close the
// statement.
//
// Exec may return ErrSkip.
//
// Deprecated: Drivers should implement ExecerContext instead (or additionally).
func (conn *HiveConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if stmt, err := NewHiveStatement(conn, query); err != nil {
		return nil, err
	} else if result, err := stmt.Exec(args); err != nil {
		return nil, err
	} else {
		return result, nil
	}
}

// ExecContext is an optional interface that may be implemented by a Conn.
//
// If a Conn does not implement ExecerContext, the sql package's DB.Exec will
// first prepare a query, execute the statement, and then close the
// statement.
//
// ExecerContext may return ErrSkip.
//
// ExecerContext must honor the context timeout and return when the context is canceled.
func (conn *HiveConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if stmt, err := NewHiveStatement(conn, query); err != nil {
		return nil, err
	} else if result, err := stmt.ExecContext(ctx, args); err != nil {
		return nil, err
	} else {
		return result, nil
	}
}

// Query is an optional interface that may be implemented by a Conn.
//
// If a Conn does not implement Queryer, the sql package's DB.Query will
// first prepare a query, execute the statement, and then close the
// statement.
//
// Query may return ErrSkip.
//
// Deprecated: Drivers should implement QueryerContext instead (or additionally).
func (conn *HiveConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	if stmt, err := NewHiveStatement(conn, query); err != nil {
		return nil, err
	} else if rows, err := stmt.Query(args); err != nil {
		return nil, err
	} else {
		return rows, nil
	}
}

// QueryContext is an optional interface that may be implemented by a Conn.
//
// If a Conn does not implement QueryerContext, the sql package's DB.Query will
// first prepare a query, execute the statement, and then close the
// statement.
//
// QueryerContext may return ErrSkip.
//
// QueryerContext must honor the context timeout and return when the context is canceled.
func (conn *HiveConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if stmt, err := NewHiveStatement(conn, query); err != nil {
		return nil, err
	} else if rows, err := stmt.QueryContext(ctx, args); err != nil {
		return nil, err
	} else {
		return rows, nil
	}
}

func (conn *HiveConn) closeSession() error {
	if conn.SessionHandle != nil {
		req := &rpc.TCloseSessionReq{
			SessionHandle: conn.SessionHandle,
		}
		_, err := conn.Client.CloseSession(req)
		if err != nil {
			log.WithFields(log.Fields{
				"reason": err,
			}).Error("Hive: close conn session failed")
			return err
		}
		conn.SessionHandle = nil
	}
	log.Debug("Hive: close session handler success")
	return nil
}

func (conn *HiveConn) resetSession() (*rpc.TSessionHandle, error) {
	conn.lock.Lock()
	defer conn.lock.Unlock()

	if err := conn.closeSession(); err != nil {
		return nil, err
	} else if err := conn.openSession(); err != nil {
		return nil, err
	}
	return conn.SessionHandle, nil
}

func (conn *HiveConn) openSession() error {
	cfg := map[string]string{
		"use:database": conn.Config.DBName,
	}
	sessionReq := &rpc.TOpenSessionReq{
		ClientProtocol: rpc.TProtocolVersion_HIVE_CLI_SERVICE_PROTOCOL_V9,
		Configuration:  cfg,
	}

	if len(conn.Config.User) > 0 {
		sessionReq.Username = &conn.Config.User
	}
	if len(conn.Config.Password) > 0 {
		sessionReq.Password = &conn.Config.Password
	}

	var (
		resp *rpc.TOpenSessionResp
		err  error
	)

	if conn.Config.DialTimeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), conn.Config.DialTimeout)
		defer cancel()
		recvChan := make(chan error)
		go func() {
			resp, err = conn.Client.OpenSession(sessionReq)
			recvChan <- err
		}()
		select {
		case <-ctx.Done():
			log.WithFields(log.Fields{
				"reason":   "timeout",
				"duration": conn.Config.DialTimeout,
			}).Error("Hive: Open session timeout")
			return errors.New("open session timeout")
		case <-recvChan:
		}
	} else {
		resp, err = conn.Client.OpenSession(sessionReq)
	}

	if err != nil {
		log.WithFields(log.Fields{
			"reason": err,
		}).Error("Hive: Error open session")
		return err
	} else if err := VerifySuccess(resp.GetStatus(), false); err != nil {
		log.WithFields(log.Fields{
			"reason": err,
		}).Error("Hive: Verify open session resp failed")
		return err
	} else if resp.GetSessionHandle() != nil {
		log.Error("Hive: Open hive session success without handler")
		return errors.New("open session success without handler")
	} else {
		log.Debug("Hive: Open hive session success")
		conn.SessionHandle = resp.GetSessionHandle()
		return nil
	}
}

func (conn *HiveConn) checkSession() error {
	req := &rpc.TGetInfoReq{
		SessionHandle: conn.SessionHandle,
		InfoType:      rpc.TGetInfoType_CLI_SERVER_NAME,
	}

	if resp, err := conn.Client.GetInfo(req); err != nil {
		log.WithFields(log.Fields{
			"reason": err,
		}).Error("Hive: Check session failed")
		return err
	} else if err := VerifySuccess(resp.GetStatus(), true); err != nil {
		return err
	} else if !resp.IsSetInfoValue() {
		log.Error("Hive: Check session response without info")
		return errors.New("check session response success without info")
	} else {
		return nil
	}
}

func (conn *HiveConn) buildSaslClient() (sasl.Client, error) {
	cfg := conn.Config
	switch cfg.Auth {
	case HiveAuthKerberos:
		return nil, errors.New("not implemented yet")
	default:
		return sasl.NewPlainClient("", cfg.User, []byte(cfg.Password))
	}
}
