package dumper

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/nakagami/firebirdsql"
)

func GetConn() *sql.DB {

	dbUser := os.Getenv("DB_USER")
	dbPass := os.Getenv("DB_PASS")
	dbHost := os.Getenv("DB_HOST")
	dbPath := os.Getenv("DB_PATH")

	strconn := fmt.Sprintf("%s:%s@%s/%s", dbUser, dbPass, dbHost, dbPath)
	conn,err := sql.Open("firebirdsql", strconn)

	if err != nil {
		log.Fatal(err)
	}
	return conn
}
