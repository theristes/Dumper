package dumper

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/nakagami/firebirdsql"
)

func GetFirebirdConn() *sql.DB {

	dbUser := os.Getenv("FIREBIRD_USER")
	dbPass := os.Getenv("FIREBIRD_PASSWORD")
	dbHost := os.Getenv("FIREBIRD_HOST")
	dbPath := os.Getenv("FIREBIRD_PATH")

	strconn := fmt.Sprintf("%s:%s@%s/%s", dbUser, dbPass, dbHost, dbPath)
	conn, err := sql.Open("firebirdsql", strconn)

	if err != nil {
		log.Fatal(err)
	}
	return conn
}

func GetMySQLConn() *sql.DB {

	dbUser := os.Getenv("MYSQL_USER")
	dbPass := os.Getenv("MYSQL_PASSWORD")
	dbHost := os.Getenv("MYSQL_HOST")
	dbPath := os.Getenv("MYSQL_PATH")

	strconn := fmt.Sprintf("%s:%s@tcp(%s)/%s", dbUser, dbPass, dbHost, dbPath)
	conn, err := sql.Open("mysql", strconn)

	if err != nil {
		log.Print("Error Connecting to MySql")
		return nil
	}
	return conn
}
