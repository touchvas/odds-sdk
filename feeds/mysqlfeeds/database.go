package mysqlfeeds

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// DbInstance gets MySQL DB Instance
func DbInstance() *sql.DB {
	username := os.Getenv("FEEDS_DATABASE_USERNAME")
	password := os.Getenv("FEEDS_DATABASE_PASSWORD")
	dbname := os.Getenv("FEEDS_DATABASE_NAME")
	host := os.Getenv("FEEDS_DATABASE_HOST")
	port := os.Getenv("FEEDS_DATABASE_PORT")

	dbURI := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=True&multiStatements=true",
		username, password, host, port, dbname)

	Db, err := sql.Open("mysql", dbURI)
	if err != nil {
		log.Fatalf("Error opening database connection: %v", err)
	}

	idleConnection := os.Getenv("FEEDS_DATABASE_IDLE_CONNECTION")
	ic, err := strconv.Atoi(idleConnection)
	if err != nil {
		ic = 5
	}

	maxConnection := os.Getenv("FEEDS_DATABASE_MAX_CONNECTION")
	mx, err := strconv.Atoi(maxConnection)
	if err != nil {
		mx = 10
	}

	connectionLifetime := os.Getenv("FEEDS_DATABASE_CONNECTION_LIFETIME")
	cl, err := strconv.Atoi(connectionLifetime)
	if err != nil {
		cl = 60
	}

	Db.SetMaxIdleConns(ic)
	Db.SetConnMaxLifetime(time.Second * time.Duration(cl))
	Db.SetMaxOpenConns(mx)
	Db.SetConnMaxIdleTime(time.Second * time.Duration(cl))

	err = Db.Ping()
	if err != nil {
		log.Fatalf("Error pinging database: %v", err)
	}

	_, err = Db.Exec("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))")
	if err != nil {

		log.Printf("error disabling ONLY_FULL_GROUP_BY %s", err.Error())
	}

	return Db
}
