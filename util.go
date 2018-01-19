package gohive

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/jellybean4/gohive/hive_service/rpc"
	log "github.com/sirupsen/logrus"
)

// REFLECT_MAP is type map from hive to golang
var REFLECT_MAP = map[string]reflect.Type{
	"BOOLEAN":             reflect.TypeOf(true),
	"TINYINT":             reflect.TypeOf(int8(0)),
	"SMALLINT":            reflect.TypeOf(int16(0)),
	"INT":                 reflect.TypeOf(int(0)),
	"BIGINT":              reflect.TypeOf(int64(0)),
	"FLOAT":               reflect.TypeOf(float32(0)),
	"DOUBLE":              reflect.TypeOf(float64(0)),
	"STRING":              reflect.TypeOf(string("")),
	"TIMESTAMP":           reflect.TypeOf(int(0)),
	"BINARY":              reflect.TypeOf([]byte{}),
	"ARRAY":               reflect.TypeOf([]interface{}{}),
	"MAP":                 reflect.TypeOf(map[string]interface{}{}),
	"STRUCT":              reflect.TypeOf(map[string]interface{}{}),
	"UNIONTYPE":           nil,
	"DECIMAL":             reflect.TypeOf(int(0)),
	"NULL":                reflect.TypeOf(nil),
	"DATE":                reflect.TypeOf(time.Time{}),
	"VARCHAR":             reflect.TypeOf(string("")),
	"CHAR":                reflect.TypeOf(rune(' ')),
	"INTERVAL_YEAR_MONTH": reflect.TypeOf(time.Time{}),
	"INTERVAL_DAY_TIME":   reflect.TypeOf(time.Time{}),
}

// VerifySuccess verifies if the status is success
func VerifySuccess(status *rpc.TStatus, withInfo bool) error {
	if (status.GetStatusCode() == rpc.TStatusCode_SUCCESS_STATUS) ||
		(withInfo && (status.GetStatusCode() == rpc.TStatusCode_SUCCESS_WITH_INFO_STATUS)) {
		return nil
	}
	log.WithFields(log.Fields{
		"errorCode": status.GetErrorCode(),
		"sqlState":  status.GetSqlState(),
		"status":    status.GetStatusCode().String(),
	}).Error("verify status failed")
	return fmt.Errorf("Hive: Verify status failed with state: %s, code: %d", status.GetSqlState(), status.GetErrorCode())
}

// SplitWith split the given string with sep according to sql syntax
func SplitWith(data string, sep rune) []string {
	quoteCount, escapeCount, lastIdx, counter := 0, 0, 0, 0
	parts := make([]string, 0)
	for i, c := range data {
		if c == '\'' {
			quoteCount++
		} else if c == '\\' {
			escapeCount++
		} else if (c == sep) && (quoteCount%2 == 0) && (escapeCount%2 == 0) {
			parts = append(parts, data[lastIdx:i])
			counter++
			lastIdx = i + 1
		}
	}

	if lastIdx < len(data) {
		parts = append(parts, data[lastIdx:])
	}
	return parts
}

// WithLock execute f with given lock
func WithLock(lock *sync.Mutex, f func()) {
	lock.Lock()
	defer lock.Unlock()
	f()
}

// PairResult represents common func result
type PairResult struct {
	value interface{}
	err   error
}

// NewPairResultChan make a new chan for PairResult
func NewPairResultChan() chan *PairResult {
	return make(chan *PairResult)
}

// NamedValueToValue parse NamedValue into Value
func NamedValueToValue(args []driver.NamedValue) []driver.Value {
	result := make([]driver.Value, len(args))
	for i := range args {
		result[i] = args[i].Value
	}
	return result
}
