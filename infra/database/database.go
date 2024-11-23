package database

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	db_configs "travel-share.com/serverless/configs/db"
)

type DBEngine = string
type DBName = string

var (
	MYSQL    DBEngine = "mysql"
	POSTGRES DBEngine = "postgres"
)

var (
	SlaveDB  DBName = "SLAVE_DB"
	MasterDB DBName = "Master_DB"
)

type ConnectOption struct {
	Engine   DBEngine
	Database DBName
}

type mysqlDB struct {
	name DBName
}

type postgresDB struct {
	name DBName
}

// 특정 DB에 종속되지 않게 connector interface로 구현
type connectorInterface interface {
	Connector() (*sqlx.DB, error)
}

var (
	mysqlSlave  *sqlx.DB
	mysqlMaster *sqlx.DB

	postgresSlave  *sqlx.DB
	postgresMaster *sqlx.DB
)

func (d *mysqlDB) Connector() (*sqlx.DB, error) {
	switch d.name {
	case SlaveDB:
		{
			if mysqlSlave != nil {
				return mysqlSlave, nil
			}
			db, err := mysqlSlaveConnector()
			mysqlSlave = db
			return db, err
		}
	case MasterDB:
		{
			if mysqlMaster != nil {
				return mysqlMaster, nil
			}
			db, err := mysqlMasterConnector()
			mysqlMaster = db
			return db, err
		}
	}
	return nil, errors.New("not supported db")
}

func mysqlSlaveConnector() (*sqlx.DB, error) {
	option := db_configs.MysqlSlaveOption()
	db, dbErr := sqlx.Connect(option.Engine, option.User+":"+option.Password+"@tcp("+option.Host+")/"+option.Database+"?charset=utf8mb4&parseTime=True&maxAllowedPacket=0")
	if dbErr != nil {
		return nil, dbErr
	}
	db.SetConnMaxLifetime(time.Minute)
	db.SetConnMaxIdleTime(time.Minute)
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(10)
	return db, nil
}

func mysqlMasterConnector() (*sqlx.DB, error) {
	option := db_configs.MysqlMasterOption()
	db, dbErr := sqlx.Connect(option.Engine, option.User+":"+option.Password+"@tcp("+option.Host+")/"+option.Database+"?charset=utf8mb4&parseTime=True&maxAllowedPacket=0")
	if dbErr != nil {
		return nil, dbErr
	}
	db.SetConnMaxLifetime(time.Minute)
	db.SetConnMaxIdleTime(time.Minute)
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(10)
	return db, nil
}

func (d *postgresDB) Connector() (*sqlx.DB, error) {
	switch d.name {
	case SlaveDB:
		{
			if postgresSlave != nil {
				return postgresSlave, nil
			}
			db, err := postgresSlaveConnector()
			postgresSlave = db
			return db, err
		}
	case MasterDB:
		{
			if postgresMaster != nil {
				return postgresMaster, nil
			}
			db, err := postgresMasterConnector()
			postgresMaster = db
			return db, err
		}
	}
	return nil, errors.New("not supported db")
}

func postgresSlaveConnector() (*sqlx.DB, error) {
	option := db_configs.PostgresSlaveOption()
	db, dbErr := sqlx.Connect(option.Engine, fmt.Sprintf("user=%s password=%s host=%s dbname=%s sslmode=disable", option.User, option.Password, option.Host, option.Database))
	if dbErr != nil {
		return nil, dbErr
	}
	db.SetConnMaxLifetime(time.Minute)
	db.SetConnMaxIdleTime(time.Minute)
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(10)
	return db, dbErr
}

func postgresMasterConnector() (*sqlx.DB, error) {
	option := db_configs.PostgresMasterOption()
	db, dbErr := sqlx.Connect(option.Engine, fmt.Sprintf("user=%s password=%s host=%s dbname=%s sslmode=disable", option.User, option.Password, option.Host, option.Database))
	if dbErr != nil {
		return nil, dbErr
	}
	db.SetConnMaxLifetime(time.Minute)
	db.SetConnMaxIdleTime(time.Minute)
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(10)
	return db, dbErr
}

func DBConnect(option ConnectOption) *sqlx.DB {
	var conn connectorInterface
	// engine 에 따라 처리
	switch option.Engine {
	case MYSQL:
		conn = &mysqlDB{
			name: option.Database,
		}
	case POSTGRES:
		conn = &postgresDB{
			name: option.Database,
		}
	default:
		log.Panicln("not support db engine")
	}
	db, err := conn.Connector()
	if err != nil {
		log.Panicln(err)
	}
	return db
}
