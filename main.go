package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

func getColumnNames(db *sql.DB, tableName string) ([]string, error) {
	query := fmt.Sprintf("SHOW COLUMNS FROM %s", tableName)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			log.Fatalf("Error closing rows: %v\n", err)
		}
	}(rows)

	var columns []string
	for rows.Next() {
		var field, fieldType, null, key, defaultValue, extra sql.NullString
		err := rows.Scan(&field, &fieldType, &null, &key, &defaultValue, &extra)
		if err != nil {
			return nil, err
		}
		columns = append(columns, field.String)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return columns, nil
}

type TableHash struct {
	Count int
	Hash  string
}

func getTableHash(db *sql.DB, tableName string) (*TableHash, error) {
	columns, err := getColumnNames(db, tableName)
	if err != nil {
		return nil, err
	}

	concatColumns := strings.Join(columns, ", ")
	newQuery := fmt.Sprintf(`
		SELECT 
			COUNT(*),
			SUM(CAST(CONV(SUBSTRING(MD5(CONCAT_WS(%s)), 18), 16, 10) AS UNSIGNED)) AS hash
		FROM %s`, concatColumns, tableName)

	println(newQuery)

	resultRow := db.QueryRow(newQuery)
	var count int
	var hash string
	err = resultRow.Scan(&count, &hash)
	if err != nil {
		return nil, err
	}
	return &TableHash{Count: count, Hash: hash}, nil
}

func NewDb(name string) (*sql.DB, error) {
	dbUser := "root"
	dbPass := "lucapette"
	dbHost := "localhost"
	dbPort := "3306"

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUser, dbPass, dbHost, dbPort, name)

	return sql.Open("mysql", dsn)
}

func main() {
	dbSource, err := NewDb("ribosom")
	if err != nil {
		log.Fatalf("Error connecting to the database: %v\n", err)
	}

	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Fatalf("Error closing database: %v\n", err)
		}
	}(dbSource)

	if err := dbSource.Ping(); err != nil {
		log.Fatalf("Error connecting to the database: %v\n", err)
	}
	dbTarget, err := NewDb("ribosom_sync")
	if err != nil {
		log.Fatalf("Error connecting to the database: %v\n", err)
	}

	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Fatalf("Error closing database: %v\n", err)
		}
	}(dbTarget)

	if err := dbTarget.Ping(); err != nil {
		log.Fatalf("Error connecting to the database: %v\n", err)
	}

	tableNames := os.Args[1:]

	for _, tableName := range tableNames {
		sourceTableHash, err := getTableHash(dbSource, tableName)
		if err != nil {
			log.Fatalf("Error getting table hash: %v\n", err)
		}
		targetTableHash, err := getTableHash(dbTarget, tableName)
		if err != nil {
			log.Fatalf("Error getting table hash: %v\n", err)
		}

		if sourceTableHash.Hash != targetTableHash.Hash {
			fmt.Printf("%s %s %d %s\n", "ribosom", tableName, sourceTableHash.Count, sourceTableHash.Hash)
			fmt.Printf("%s %s %d %s\n", "ribosom_sync", tableName, targetTableHash.Count, targetTableHash.Hash)
		}
	}
}
