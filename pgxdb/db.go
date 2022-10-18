package pgxdb

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	// Postgres access
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // github.com/lib/pq
	"github.com/jmoiron/sqlx"
)

type Querier interface {
	Select(interface{}, string, ...interface{}) error
}

type Entity interface {
	TableName() string
}

type DB struct {
	*sqlx.DB
}

func NewDB(url string) (*DB, error) {
	db, err := sqlx.Open("pgx", url)
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

func NewDBPool(ctx context.Context, url string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	return pool, nil
}

func InsertSkip(db sqlx.Ext, r Entity, skip []string) (sql.Result, error) {
	return insert(db, r, skip)
}

func Insert(db sqlx.Ext, r Entity) (sql.Result, error) {
	return insert(db, r, nil)
}

func (db *DB) Insert(r Entity) (sql.Result, error) {
	return insert(db, r, nil)
}

func insert(db sqlx.Ext, e Entity, skip []string) (sql.Result, error) {
	fields, key, err := Fields(e, skip) // e.g. []string{"id", "name", "description"}
	if err != nil {
		return nil, err
	}
	csv := FieldsCSV(fields)       // e.g. "id, name, description"
	csvc := FieldsCSVColon(fields) // e.g. ":id, :name, :description"
	sql := "INSERT INTO " + e.TableName() + " (" + csv + ") VALUES (" + csvc + ") RETURNING " + key
	fmt.Printf("--- SQL: %s, entity: %v\n", sql, e)
	rows, err := sqlx.NamedQuery(db, sql, e)

	// read the last insert id and total rows affected
	var r InsertResult
	if err == nil {
		var total int64
		for rows.Next() {
			rows.Scan(&r.lastID)
			total++
		}
		r.rowsAffected = total
	}
	// r, err := sqlx.NamedExec(db, sql, e)
	if err == nil {
		count, err := r.RowsAffected()
		if count <= 0 || err != nil {
			return &r, fmt.Errorf("insert rowsaffected Error: %v\n", err)
		}
	}
	return &r, err
}

type InsertResult struct {
	lastID       int64
	rowsAffected int64
}

func (r *InsertResult) LastInsertId() (int64, error) {
	return r.lastID, nil
}
func (r *InsertResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

func Update(db sqlx.Ext, r Entity, fields []string) (sql.Result, error) {
	return update(db, r, fields)
}

func (db *DB) Update(r Entity, fields []string) (sql.Result, error) {
	return update(db, r, fields)
}

// fields - e.g. []string{Name", "Description"}
func update(db sqlx.Ext, e Entity, fields []string) (sql.Result, error) {
	dbFields, key, err := DBTags(e, fields)
	if err != nil {
		return nil, err
	}
	set := FieldsSet(dbFields) // e.g. "name = :name, description = :description"
	sql := "UPDATE " + e.TableName() + " set " + set + " where " + key + " = :" + key
	fmt.Printf("--- SQL: %s, entity: %v\n", sql, e)
	r, err := sqlx.NamedExec(db, sql, e)
	if err == nil {
		count, err := r.RowsAffected()
		if count <= 0 || err != nil {
			return r, fmt.Errorf("update rowsaffected Error: %v\n", err)
		}
	}
	return r, err
}

func DBTags(s interface{}, fieldNames []string) ([]string, string, error) {
	v := reflect.ValueOf(s)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, "", fmt.Errorf("DBTags requires a struct, found: %s", v.Kind().String())
	}
	primaryKey := ""
	fields := []string{}
	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i)
		dbName := field.Tag.Get("db")
		gorm := field.Tag.Get("gorm")
		if field.Name == "ID" || gorm == "primary_key" {
			primaryKey = dbName
		}
		if SliceContains(fieldNames, field.Name) {
			fields = append(fields, dbName)
		}
	}
	if primaryKey == "" {
		return nil, "", fmt.Errorf("DBTags Primary key not found! Struct: %v, fields: %v", s, fieldNames)
	}
	return fields, primaryKey, nil
}

// DBFields reflects on a struct and returns the values of fields with `db` tags,
// or a map[string]interface{} and returns the keys.
func Fields(values interface{}, skip []string) ([]string, string, error) {
	v := reflect.ValueOf(values)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	primaryKey := ""
	fields := []string{}
	if v.Kind() == reflect.Struct {
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			dbName := field.Tag.Get("db")
			gorm := field.Tag.Get("gorm")
			if field.Name == "ID" || gorm == "primary_key" {
				primaryKey = dbName
			}
			if dbName != "" && !SliceContains(skip, dbName) {
				fields = append(fields, dbName)
			}
		}
		if primaryKey == "" {
			return nil, "", fmt.Errorf("Fields Primary key not found! Struct: %v", values)
		}
		return fields, primaryKey, nil
	}
	if v.Kind() == reflect.Map {
		for _, keyv := range v.MapKeys() {
			fields = append(fields, keyv.String())
		}
		return fields, "", nil
	}
	panic(fmt.Errorf("Fields requires a struct or a map, found: %s", v.Kind().String()))
}

func FieldsCSV(fields []string) string {
	return fieldsCSV(fields, false, true)
}

func FieldsCSVColon(fields []string) string {
	return fieldsCSV(fields, true, false)
}

func fieldsCSV(fields []string, colon bool, escape bool) string {
	result := ""
	for i := 0; i < len(fields); i++ {
		field := fields[i]
		if i > 0 {
			result += ", "
		}
		if colon {
			result += ":"
		}
		if escape {
			result += "\"" + field + "\""
		} else {
			result += field
		}
	}
	return result
}

func FieldsSet(fields []string) string {
	result := ""
	for i := 0; i < len(fields); i++ {
		field := fields[i]
		if i > 0 {
			result += ", "
		}
		result += field + " = :" + field
	}
	return result
}

func Exec(tx *sqlx.Tx, query string, args ...any) (int64, error) {
	fmt.Printf("--- SQL: %s, params: %v\n", query, args)
	r, err := tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	count, err := r.RowsAffected()
	if count <= 0 || err != nil {
		return 0, fmt.Errorf("No rows affected! Error: %w\n", err)
	}
	return count, nil
}
