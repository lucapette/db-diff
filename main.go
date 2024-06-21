package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"strings"
)

func getColumnNames(db *sql.DB, tableName string, includedColumns []string, excludedColumns []string) ([]string, error) {
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

	if len(includedColumns) > 0 {
		var filteredColumns []string
		for _, column := range columns {
			for _, includedColumn := range includedColumns {
				if column == includedColumn {
					filteredColumns = append(filteredColumns, column)
				}
			}
		}
		columns = filteredColumns
	}

	if len(excludedColumns) > 0 {
		var filteredColumns []string
		for _, column := range columns {
			exclude := false
			for _, excludedColumn := range excludedColumns {
				if column == excludedColumn {
					exclude = true
				}
			}
			if !exclude {
				filteredColumns = append(filteredColumns, column)
			}
		}
		columns = filteredColumns
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return columns, nil
}

type ChunkHash struct {
	Count int
	Hash  string
}

type TableInfo struct {
	TableName string
	Columns   []string
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

func getChunkHash(db *sql.DB, tableInfo *TableInfo, minId int64, maxId int64) (*ChunkHash, error) {
	newQuery := fmt.Sprintf(`
		SELECT 
			COUNT(*),
			SUM(CAST(CONV(SUBSTRING(MD5(CONCAT_WS(%s)), 18), 16, 10) AS UNSIGNED)) AS hash
		FROM %s
		WHERE id >= %d AND id <= %d
		ORDER BY id
		`, strings.Join(tableInfo.Columns, ","), tableInfo.TableName, minId, maxId)

	resultRow := db.QueryRow(newQuery)
	var count int
	var hash string
	err := resultRow.Scan(&count, &hash)
	if err != nil {
		return nil, err
	}
	return &ChunkHash{Count: count, Hash: hash}, nil
}

func NewDb(dsn string) (*sql.DB, error) {
	return sql.Open("mysql", dsn)
}

func compareRows(dbSource *sql.DB, dbTarget *sql.DB, tableInfo *TableInfo, minId int64, maxId int64) []int64 {
	query := fmt.Sprintf(
		`SELECT id, CAST(CONV(SUBSTRING(MD5(CONCAT_WS(%s)), 18), 16, 10) AS UNSIGNED) AS hash FROM %s WHERE id >= %d AND id <= %d`,
		strings.Join(tableInfo.Columns, ","), tableInfo.TableName, minId, maxId)

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
	var source = flag.String("source", "", "Source database connection string")
	var target = flag.String("target", "", "Target database connection string")
	var tableName = flag.String("table", "", "Table name to compare")
	var exclude = flag.String("exclude", "", "Comma separated list of columns to exclude from comparison")
	var include = flag.String("include", "", "Comma separated list of columns to include in comparison")

	flag.Parse()

	if *source == "" || *target == "" || *tableName == "" {
		log.Fatalf("source, target and table are required\n")
	}

	if *include != "" && *exclude != "" {
		log.Fatalf("include and exclude can't be used together\n")
	}

	dbSource, err := NewDb(*source)
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

	dbTarget, err := NewDb(*target)
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

	var chunkSize int64 = 10000

	var includeColumns []string

	if *include != "" {
		includeColumns = strings.Split(*include, ",")
	}

	var excludeColumns []string
	if *exclude != "" {
		excludeColumns = strings.Split(*exclude, ",")
	}

	tableColumns, err := getColumnNames(dbSource, *tableName, includeColumns, excludeColumns)
	if err != nil {
		log.Fatalf("Error getting column names: %v\n", err)
	}
	tableInfo := &TableInfo{TableName: *tableName, Columns: tableColumns}

	sourceMinId, sourceMaxId, err := getMinMax(dbSource, *tableName)
	if err != nil {
		log.Fatalf("Error getting min and max id: %v\n", err)
	}

	ids := make([]int64, 0)
	for i := sourceMinId; i <= sourceMaxId; i += chunkSize {
		sourceTableHash, err := getChunkHash(dbSource, tableInfo, i, i+chunkSize-1)
		if err != nil {
			log.Fatalf("Error getting table hash: %v\n", err)
		}

		targetTableHash, err := getChunkHash(dbTarget, tableInfo, i, i+chunkSize-1)
		if err != nil {
			log.Fatalf("Error getting table hash: %v\n", err)
		}

		if sourceTableHash.Hash != targetTableHash.Hash {
			ids = append(ids, compareRows(dbSource, dbTarget, tableInfo, i, i+chunkSize-1)...)
		}
	}

	if len(ids) > 0 {
		fmt.Printf("%d rows for table %s differ\n", len(ids), *tableName)
	}
}
