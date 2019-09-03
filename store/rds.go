package store

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"sync"
	"github.com/a-h/go-sql-driver-rds-credentials/store/certs"
	"github.com/go-sql-driver/mysql"
)

type secretGetter interface {
	Get(force bool) (secret string, err error)
	CallsMade() int
}

// RDS store, backed by AWS Secrets Manager.
type RDS struct {
	dsn      *DSN
	config   *mysql.Config
	m        *sync.Mutex
}

func NewRDS(dbName string, dbHost string, dbPort string, dbUser string, params map[string]string, credsCB CredsCallbackFn) (rds *RDS, err error) {
	mysqlConf := mysql.NewConfig()
	mysqlConf.DBName = dbName
	mysqlConf.Params = params

	// Load the TLS certificates.
	var pem []byte
	pem, err = certs.Load()
	if err != nil {
		err = fmt.Errorf("store: could not load certificates: %v", err)
		return
	}
	rcp := x509.NewCertPool()
	if ok := rcp.AppendCertsFromPEM(pem); !ok {
		err = errors.New("store: could not append certificates from PEM")
		return
	}
	mysql.RegisterTLSConfig("rds", &tls.Config{
		RootCAs: rcp,
	})
	mysqlConf.Params["tls"] = "rds"

	rds = &RDS{
		dsn:    NewDSN(mysqlConf, dbHost, dbPort, dbUser, credsCB),
		config: mysqlConf,
		m:      &sync.Mutex{},
	}

	// force the first connection
	_, err = rds.Get(true)
	if err != nil {
		err = fmt.Errorf("could not get first DSN: %v", err)
		return nil, err
	}

	return
}

// Get the DSN, optionally forcing a refresh.
func (s *RDS) Get(force bool) (string, error) {
	dsn, err := s.dsn.Get(force)
	if err != nil {
		return "", err
	}

	return dsn, nil
}

// CallsMade to the underlying secret API.
func (s *RDS) CallsMade() int {
	return s.dsn.CallsMade()
}

type rdsSecret struct {
	Username            string `json:"username"`
	Password            string `json:"password"`
	Engine              string `json:"engine"`
	Host                string `json:"host"`
	Port                int    `json:"port"`
	DbClusterIdentifier string `json:"dbClusterIdentifier"`
}
