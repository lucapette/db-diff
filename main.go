package main

import (
	"database/sql"
	"fmt"
	"log"
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

func getMinMax(db *sql.DB, name string) (int64, int64, error) {
	query := fmt.Sprintf("SELECT MIN(id), MAX(id) FROM %s", name)
	row := db.QueryRow(query)
	var minId, maxId int64
	err := row.Scan(&minId, &maxId)
	if err != nil {
		return 0, 0, err
	}
	return minId, maxId, nil
}

func getTableHash(db *sql.DB, tableName string, minId int64, maxId int64) (*TableHash, error) {
	columns, err := getColumnNames(db, tableName)
	if err != nil {
		return nil, err
	}

	concatColumns := strings.Join(columns, ", ")
	newQuery := fmt.Sprintf(`
		SELECT 
			COUNT(*),
			SUM(CAST(CONV(SUBSTRING(MD5(CONCAT_WS(%s)), 18), 16, 10) AS UNSIGNED)) AS hash
		FROM %s
		WHERE id >= %d AND id <= %d
		ORDER BY id
		`, concatColumns, tableName, minId, maxId)

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

func compareRows(dbSource *sql.DB, dbTarget *sql.DB, tableName string, minId int64, maxId int64) []int64 {
	columns, err := getColumnNames(dbSource, tableName)
	if err != nil {
		log.Fatalf("Error getting column names: %v\n", err)
	}

	concatColumns := strings.Join(columns, ", ")
	query := fmt.Sprintf("SELECT id, CAST(CONV(SUBSTRING(MD5(CONCAT_WS(%s)), 18), 16, 10) AS UNSIGNED) AS hash FROM %s WHERE id >= %d AND id <= %d", concatColumns, tableName, minId, maxId)
	sourceRows, err := dbSource.Query(query)
	if err != nil {
		log.Fatalf("Error querying the database: %v\n", err)
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			log.Fatalf("Error closing rows: %v\n", err)
		}
	}(sourceRows)

	targetRows, err := dbTarget.Query(query)
	if err != nil {
		log.Fatalf("Error querying the database: %v\n", err)
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			log.Fatalf("Error closing rows: %v\n", err)
		}
	}(targetRows)

	sourceRowHashes := make(map[int64]string)

	for sourceRows.Next() {
		var sourceId int64
		var hash string
		err = sourceRows.Scan(&sourceId, &hash)
		if err != nil {
			log.Fatalf("Error scanning source rows: %v\n", err)
		}
		sourceRowHashes[sourceId] = hash
	}

	ids := make([]int64, 0)

	for targetRows.Next() {
		var targetId int64
		var hash string
		err = targetRows.Scan(&targetId, &hash)
		if err != nil {
			log.Fatalf("Error scanning target rows: %v\n", err)
		}
		sourceHash, ok := sourceRowHashes[targetId]
		if !ok {
			ids = append(ids, targetId)
			continue
		}
		if sourceHash != hash {
			ids = append(ids, targetId)
		}
	}
	return ids
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

	tableNames := []string{"reference"}

	for _, tableName := range tableNames {
		var chunkSize int64 = 10000

		sourceMinId, sourceMaxId, err := getMinMax(dbSource, tableName)
		if err != nil {
			log.Fatalf("Error getting min and max id: %v\n", err)
		}

		ids := make([]int64, 0)
		for i := sourceMinId; i <= sourceMaxId; i += chunkSize {
			sourceTableHash, err := getTableHash(dbSource, tableName, i, i+chunkSize-1)
			if err != nil {
				log.Fatalf("Error getting table hash: %v\n", err)
			}

			targetTableHash, err := getTableHash(dbTarget, tableName, i, i+chunkSize-1)
			if err != nil {
				log.Fatalf("Error getting table hash: %v\n", err)
			}

			if sourceTableHash.Hash != targetTableHash.Hash {
				ids = append(ids, compareRows(dbSource, dbTarget, tableName, i, i+chunkSize-1)...)
			}
		}
		fmt.Printf("%d rows for table %s differ\n", len(ids), tableName)
	}

}
