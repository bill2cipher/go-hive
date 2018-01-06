package gohive

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"time"
)

type Config struct {
	Scheme       string
	Host         string
	Port         int64
	User         string
	Password     string
	DBName       string
	DialTimeout  time.Duration
	ExecTimeout  time.Duration
	Auth         string
	KerberosName string
	RunAsync     bool
}

const (
	DEFAULT_HOST        = "locahost"
	DEFAULT_PORT        = 10000
	HIVE_2              = "hive2"
	PARAM_DIAL_TIMEOUT  = "dialTimeout"
	PARAM_EXEC_TIMEOUT  = "execTimeout"
	PARAM_AUTH          = "auth"
	PARAM_KERBEROS_NAME = "kerberos_name"
	PARAM_RUN_ASYNC     = "runAsync"
)

const (
	AUTH_LDAP     = "ldap"
	AUTH_KERBEROS = "kerberos"
)

var (
	DefaultTimeout                  = 30 * int64(math.Pow10(9))
	ERROR_INVALID_SCHEME            = errors.New("Hive: Invalid scheme, only hive2 supported")
	ERROR_INVALID_CONNECTION_STRING = errors.New("Hive: Invalid connection string")
	ERROR_INVALID_PORT              = errors.New("Hive: Invalid port in connection string")
	ERROR_INVALID_DATABASE_NAME     = errors.New("Hive: Invalid database name")
	ERROR_INVALID_AUTH_TYPE         = errors.New("Hive: Invalid auth type")
	ERROR_INVALID_DIAL_TIMEOUT      = errors.New("Hive: Invalid DialTimeout")
	ERROR_INVALID_EXEC_TIMEOUT      = errors.New("Hive: Invalid ExecTimeout")
	ERROR_INVALID_ASYNC_VALUE       = errors.New("Hive: Invalid async value, must be true or false")
)

func NewConfig() *Config {
	return &Config{
		Host:        DEFAULT_HOST,
		Port:        DEFAULT_PORT,
		DialTimeout: 0,
		ExecTimeout: 0,
		RunAsync:    false,
	}
}

func (cfg *Config) Address() string {
	return fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
}

func (cfg *Config) normalize() error {
	if cfg.DialTimeout < 0 {
		return ERROR_INVALID_DIAL_TIMEOUT
	}
	if cfg.ExecTimeout < 0 {
		return ERROR_INVALID_EXEC_TIMEOUT
	}

	if (len(cfg.Password) > 0) && (cfg.Auth != AUTH_LDAP) {
		return errors.New("Hive: Password should be set if and only if in LDAP mode")
	}

	if (len(cfg.KerberosName) > 0) != (cfg.Auth == AUTH_KERBEROS) {
		return errors.New("Hive: kerberos_name should be set if and only if in KERBEROS mode")
	}

	if len(cfg.DBName) <= 0 {
		return errors.New("dbname not set within dsn")
	}
	return nil
}

func (cfg *Config) FormatDSN() string {
	u := url.URL{
		Scheme: cfg.Scheme,
		Host:   fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
	}

	// [username[:password]@]
	if (len(cfg.User) != 0) && (len(cfg.Password) == 0) {
		u.User = url.User(cfg.User)
	} else if (len(cfg.User) != 0) && (len(cfg.Password) != 0) {
		u.User = url.UserPassword(cfg.User, cfg.Password)
	}

	// dbname
	u.Path = cfg.DBName

	// [?param1=value1&...&paramN=valueN]
	values := url.Values{}
	if cfg.DialTimeout > 0 {
		values.Add(PARAM_DIAL_TIMEOUT, fmt.Sprintf("%d", cfg.DialTimeout))
	}

	if cfg.ExecTimeout > 0 {
		values.Add(PARAM_EXEC_TIMEOUT, fmt.Sprintf("%d", cfg.ExecTimeout))
	}

	if len(cfg.Auth) > 0 {
		values.Add(PARAM_AUTH, cfg.Auth)
	}

	if len(cfg.KerberosName) > 0 {
		values.Add(PARAM_KERBEROS_NAME, cfg.KerberosName)
	}

	if cfg.RunAsync {
		values.Add(PARAM_RUN_ASYNC, "true")
	}
	u.RawQuery = values.Encode()

	return u.String()
}

func ParseDSN(dsn string) (*Config, error) {
	config := NewConfig()
	if u, err := url.Parse(dsn); err != nil {
		return nil, err
	} else if err := setConfig(config, u); err != nil {
		return nil, err
	} else if err = config.normalize(); err != nil {
		return nil, err
	}
	return config, nil
}

func setConfig(config *Config, u *url.URL) error {
	config.Scheme = u.Scheme
	if config.Scheme != HIVE_2 {
		return ERROR_INVALID_SCHEME
	}

	config.DBName = u.Path

	if len(u.Hostname()) == 0 {
		config.Host = DEFAULT_HOST
	} else {
		config.Host = u.Hostname()
	}

	if len(u.Port()) == 0 {
		config.Port = DEFAULT_PORT
	} else if port, err := strconv.ParseInt(u.Port(), 10, 64); err != nil {
		return err
	} else {
		config.Port = port
	}

	if u.User != nil {
		config.User = u.User.Username()
		if password, set := u.User.Password(); set {
			config.Password = password
		}
	}

	values := u.Query()
	if kerb, ok := values[PARAM_KERBEROS_NAME]; ok {
		config.KerberosName = kerb[0]
	}

	if auth, ok := values[PARAM_AUTH]; ok {
		if auth[0] == AUTH_LDAP || auth[0] == AUTH_KERBEROS {
			config.Auth = auth[0]
		} else {
			return ERROR_INVALID_AUTH_TYPE
		}
	}
	if dialTimeout, ok := values[PARAM_DIAL_TIMEOUT]; ok {
		if timeout, err := strconv.ParseInt(dialTimeout[0], 10, 64); err != nil {
			return err
		} else {
			config.DialTimeout = time.Duration(timeout)
		}
	}
	if execTimeout, ok := values[PARAM_EXEC_TIMEOUT]; ok {
		if timeout, err := strconv.ParseInt(execTimeout[0], 10, 64); err != nil {
			return err
		} else {
			config.ExecTimeout = time.Duration(timeout)
		}
	}

	if runAsync, ok := values[PARAM_RUN_ASYNC]; ok {
		if runAsync[0] == "true" {
			config.RunAsync = true
		} else if runAsync[0] == "false" {
			config.RunAsync = false
		} else {
			return ERROR_INVALID_ASYNC_VALUE
		}
	}
	return nil
}
