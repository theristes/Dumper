package dumper

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/nakagami/firebirdsql"
	_ "github.com/go-sql-driver/mysql" 
	
)

func GetFirebirdConn() *sql.DB {

	firebirdUser := os.Getenv("FIREBIRD_USER")
	firebirdPassword := os.Getenv("FIREBIRD_PASSWORD")
	firebirdHost := os.Getenv("FIREBIRD_HOST")
	firebirdPath := os.Getenv("FIREBIRD_PATH")

	strconn := fmt.Sprintf("%s:%s@%s/%s", firebirdUser, firebirdPassword, firebirdHost, firebirdPath)
	conn, err := sql.Open("firebirdsql", strconn)

	if err != nil {
		log.Fatal(err)
	}
	return conn
}

func GetMySQLConn() *sql.DB {

	mysqlUser := os.Getenv("MYSQL_USER")
	mysqlPassword := os.Getenv("MYSQL_PASSWORD")
	mysqlHost := os.Getenv("MYSQL_HOST")
	mysqlDatabase := os.Getenv("MYSQL_DATABASE")

	strconn := fmt.Sprintf("%s:%s@tcp(%s)/%s", mysqlUser, mysqlPassword, mysqlHost, mysqlDatabase)
	conn, err := sql.Open("mysql", strconn)

	if err != nil {
		log.Print("Error Connecting to MySql")
		return nil
	}
	return conn
}
