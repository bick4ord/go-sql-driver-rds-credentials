package store

import (
	"sync"
	"time"
	"net/url"
	"github.com/aws/aws-sdk-go/service/rds/rdsutils"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/go-sql-driver/mysql"
	"fmt"
)

// DSN store
type DSN struct {
	CacheFor      time.Duration
	LastRefreshed time.Time
	m             *sync.Mutex
	Value         string
	callsMade     int
	mysqlConf     *mysql.Config
	DbHost        string
	DbPort        string
	DbUser        string
	CredsCallback CredsCallbackFn
}

// CredsCallbackFn must return AWS creds and region
type CredsCallbackFn func(*DSN)(*credentials.Credentials, *string)

// NewDSN creates a new, empty DSN store.
func NewDSN(c *mysql.Config, dbHost string, dbPort string, dbUser string, credsCallback CredsCallbackFn) *DSN {
	return &DSN{
		Value:         "",
		CacheFor:      time.Duration(14) * time.Minute,
		LastRefreshed: time.Time{},
		m:             &sync.Mutex{},
		mysqlConf:     c,
		DbHost:        dbHost,
		DbPort:        dbPort,
		DbUser:        dbUser,
		CredsCallback: credsCallback,
	}
}

func (dsn *DSN) refresh() (dsnVal string, err error) {
	v := url.Values{}

	for paramKey, paramVal := range dsn.mysqlConf.Params {
		v.Add(paramKey, paramVal)
	}
	creds, awsRegion := dsn.CredsCallback(dsn)
	csb := rdsutils.NewConnectionStringBuilder(dsn.DbHost+":"+dsn.DbPort, *awsRegion, dsn.DbUser, dsn.mysqlConf.DBName, creds)
	dsnVal, err = csb.WithTCPFormat().WithParams(v).Build()
	if err != nil {
		err = fmt.Errorf("could not build connection string: %v", err)
	}
	if dsnVal == "" {
		err = fmt.Errorf("unknown error building connection string (bad creds?)")
	}
	return
}

// Get the DSN, optionally forcing a refresh.
func (dsn *DSN) Get(force bool) (val string, err error) {
	dsn.m.Lock()
	defer dsn.m.Unlock()
	if force || dsn.Value == "" || time.Now().UTC().After(dsn.LastRefreshed.Add(dsn.CacheFor)) {
		newDSN, err := dsn.refresh()
		if err != nil {
			return "", err
		}
		dsn.callsMade++
		dsn.Value = newDSN
		dsn.LastRefreshed = time.Now().UTC()
	}
	return dsn.Value, nil
}

// CallsMade to the underlying secret API.
func (dsn *DSN) CallsMade() int {
	return dsn.callsMade
}
