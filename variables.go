package gohive

import "fmt"

const (
	HIVE_DRIVER_NAME = "hive"
)

var (
	ERROR_NOT_SUPPORTED            = fmt.Errorf("Hive: Not supported")
	ERROR_NOT_IMPLEMENTED          = fmt.Errorf("Hive: Not implemented")
	ERROR_PREPARED_QUERY_NOT_FOUND = fmt.Errorf("Hive: Prepared query not found")
	ERROR_INTERNAL                 = fmt.Errorf("Hive: Internal error")
)
