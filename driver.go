package gohive

import (
	"database/sql"
	"database/sql/driver"
	"net"
)

func init() {
	sql.Register(HIVE_2, &HiveDriver{})
}

// HiveDriver implements sql.driver
type HiveDriver struct {
}

// DialFunc is a function which can be used to establish the network connection.
// Custom dial functions must be registered with RegisterDial
type DialFunc func(addr string) (net.Conn, error)

var dials map[string]DialFunc

// RegisterDial registers a custom dial function. It can then be used by the
// network address mynet(addr), where mynet is the registered new network.
// addr is passed as a parameter to the dial function.
func RegisterDial(net string, dial DialFunc) {
	if dials == nil {
		dials = make(map[string]DialFunc)
	}
	dials[net] = dial
}

// Open new Connection.
func (d *HiveDriver) Open(dsn string) (driver.Conn, error) {
	return NewHiveConn(dsn)
}
