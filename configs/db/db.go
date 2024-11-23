package db_configs

import (
	"os"

	"github.com/joho/godotenv"
)

type ConnectOptions struct {
	Engine   string
	User     string
	Password string
	Host     string
	Database string
}

var (
	_ = godotenv.Load()
)

func MysqlSlaveOption() ConnectOptions {
	return ConnectOptions{
		Engine:   "mysql",
		User:     os.Getenv("MYSQL_SLAVE_USER"),
		Password: os.Getenv("MYSQL_SLAVE_PASSWORD"),
		Host:     os.Getenv("MYSQL_SLAVE_HOST"),
		Database: os.Getenv("MYSQL_SLAVE_DATABASE"),
	}
}

func MysqlMasterOption() ConnectOptions {
	return ConnectOptions{
		Engine:   "mysql",
		User:     os.Getenv("MYSQL_MASTER_USER"),
		Password: os.Getenv("MYSQL_MASTER_PASSWORD"),
		Host:     os.Getenv("MYSQL_MASTER_HOST"),
		Database: os.Getenv("MYSQL_MASTER_DATABASE"),
	}
}

func PostgresSlaveOption() ConnectOptions {
	return ConnectOptions{
		Engine:   "postgres",
		User:     os.Getenv("PG_SLAVE_USER"),
		Password: os.Getenv("PG_SLAVE_PASSWORD"),
		Host:     os.Getenv("PG_SLAVE_HOST"),
		Database: os.Getenv("PG_SLAVE_DATABASE"),
	}
}

func PostgresMasterOption() ConnectOptions {
	return ConnectOptions{
		Engine:   "postgres",
		User:     os.Getenv("PG_MASTER_USER"),
		Password: os.Getenv("PG_MASTER_PASSWORD"),
		Host:     os.Getenv("PG_MASTER_HOST"),
		Database: os.Getenv("PG_MASTER_DATABASE"),
	}
}
