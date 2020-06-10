package sqlssx

import (
	"database/sql"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	"github.com/Stachio/go-extdata"
	"github.com/Stachio/go-printssx"
	"github.com/fatih/structs"

	//Mysql driver
	_ "github.com/go-sql-driver/mysql"
)

// Printer - Generic printer object provided by stachio/printerssx
var Printer = printssx.New("SQLSSX", log.Println, log.Printf, printssx.Subtle, printssx.Subtle)

// Error - Package defined error struct to house sql statements
type Error struct {
	operation string
	statement string
	goerr     error
}

func (err *Error) Error() string {
	return "Operation: " + err.operation + "\nStatement: " + err.statement + "\nError: " + err.goerr.Error()
}

// NewError - returns custom error type
func NewError(operation, statement string, err error) *Error {
	return &Error{operation: operation, statement: statement, goerr: err}
}

// Database - Database struct used to encapsulate sqlssx functinos
type Database struct {
	name  string
	sqlDB *sql.DB
}

// GetName - Get function to protect name value
func (db *Database) GetName() string {
	return db.name
}

type ConfigDatabase struct {
	Name     string `xml:"name,attr"`
	User     string `xml:"user"`
	Password []byte `xml:"password"`
}

type ConfigServer struct {
	Name      string            `xml:"name,attr"`
	Port      string            `xml:"port,attr"`
	Databases []*ConfigDatabase `xml:"database"`
}

type Config struct {
	Servers []*ConfigServer `xml:"server"`
}

// Server - Server struct for multiple databases on a server
type Server struct {
	name string
	port string
	//user string

	dbCatalog     map[string]*Database //= make(map[string]*Database)
	dbPrimaryName string
	dbPrimary     *Database
}

var serverCatalog = make(map[string]*Server)

func ServerByName(name string) *Server {
	server, ok := serverCatalog[name]
	if !ok {
		return nil
	}

	return server
}

// Open - Open a pre-built database
// Note: database MUST exists
func Open(server, port, dbName, user string, pass []byte) (db *Database, err error) {
	Printer.Printf(printssx.Subtle, "Opening database %s/%s\n", server, dbName)
	openStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&collation=utf8mb4_bin", user, string(pass), server, port, dbName)
	sqlDB, erro := sql.Open("mysql", openStr)
	if erro != nil {
		return nil, NewError("DB Open", openStr, erro)
	}

	// Pretty sure we need to do a simple query to ensure the db is connected
	erro = sqlDB.Ping()
	if erro != nil {
		sqlDB.Close()
		return nil, NewError("DB Ping", "", erro)
	}

	sqlDB.SetMaxIdleConns(0)
	db = &Database{name: dbName, sqlDB: sqlDB}
	//dbCatalog[server] = make(map[string]*Database)
	ServerByName(server).dbCatalog[dbName] = db
	return
}

// NewServer - Returns a new server object
func NewServer(name, port, dbName, user string, pass []byte) (server *Server, err error) {
	Printer.Printf(printssx.Subtle, "Connecting to PRIMARY %s:%s:%s with user %s\n", name, port, dbName, user)

	server = &Server{
		name:          name,
		port:          port,
		dbPrimaryName: dbName,
		dbCatalog:     make(map[string]*Database),
	}
	serverCatalog[name] = server

	db, err := Open(name, port, dbName, user, pass)
	if err != nil {
		return
	}

	server.dbPrimary = db

	return
}

func NewServerWithConfig(server string, database string, config *Config) (*Server, error) {
	var foundServer *ConfigServer
	for _, configServer := range config.Servers {
		if configServer.Name == server {
			foundServer = configServer
			break
		}
	}
	if foundServer == nil {
		return nil, fmt.Errorf("Config file missing server %s", server)
	}

	var foundDatabase *ConfigDatabase
	for _, configDatabase := range foundServer.Databases {
		if configDatabase.Name == database {
			foundDatabase = configDatabase
			break
		}
	}
	if foundDatabase == nil {
		return nil, fmt.Errorf("Config file  server %s missing database %s", server, database)
	}
	return NewServer(server, foundServer.Port, database, foundDatabase.User, foundDatabase.Password)
}

func NewServerWithConfigFile(server string, database string, configPath string) (*Server, error) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	config := &Config{}
	err = xml.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}
	return NewServerWithConfig(server, database, config)
}

//GetName - Get function to protect name value
func (server *Server) GetName() string {
	return server.name
}

func (server *Server) DatabaseByName(name string) *Database {
	return server.dbCatalog[name]
}

// Prepare - Adapted sql prepare functionality to wrap custom error class
func (db *Database) Prepare(statement string) (sqlStatement *sql.Stmt, err error) {
	sqlStatement, erro := db.sqlDB.Prepare(statement)
	//fmt.Println(statement)
	operation := "SQL Prepare"
	if erro != nil {
		err = NewError(operation, statement, erro)
	}
	return
}

// Execute - Excecute a statement against the associated database
func (db *Database) Execute(statement string, args ...interface{}) (result sql.Result, err error) {
	Printer.Println(printssx.Loud, "Executing", statement)
	parentOp := "SQL Execute"
	sqlStatement, err := db.Prepare(statement)
	if err != nil {
		return
	}

	result, erro := sqlStatement.Exec(args...)
	if erro != nil {
		err = NewError(parentOp, statement, erro)
	}
	sqlStatement.Close()
	return
}

// Execute - Server-based execute against primary database
func (server *Server) Execute(statement string, args ...interface{}) (result sql.Result, err error) {
	result, err = server.dbPrimary.Execute(statement, args...)
	return
}

// Query - Query a statement against the associated database
// Returns *sql.Rows
func (db *Database) Query(statement string, args ...interface{}) (sqlRows *sql.Rows, err error) {
	Printer.Println(printssx.Loud, "Querying", statement, "with args", args)
	parentOp := "SQL Query"
	sqlStatement, err := db.Prepare(statement)
	if err != nil {
		return
	}

	sqlRows, erro := sqlStatement.Query(args...)
	if erro != nil {
		err = NewError(parentOp, statement, erro)
	}
	sqlStatement.Close()
	return
}

// Query - Server-based query against primary database
func (server *Server) Query(statement string, args ...interface{}) (sqlRows *sql.Rows, err error) {
	sqlRows, err = server.dbPrimary.Query(statement, args...)
	return
}

//QueryRow - Query a statement against the provided database
//Returns *sql.Row
func (db *Database) QueryRow(statement string, args ...interface{}) (sqlRow *sql.Row, err error) {
	Printer.Println(printssx.Loud, "Single query", statement, "with args", args)
	sqlStatement, err := db.Prepare(statement)
	if err != nil {
		return
	}

	sqlRow = sqlStatement.QueryRow(args...)
	sqlStatement.Close()
	return
}

// QueryRow - Server-based queryrow against primary database
func (server *Server) QueryRow(statement string, args ...interface{}) (sqlRow *sql.Row, err error) {
	sqlRow, err = server.dbPrimary.QueryRow(statement, args...)
	return
}

type Condition struct {
	Statement string
	Glue      string
}

func glueConditions(conditions []Condition) string {
	var statement string
	for _, cond := range conditions {
		statement += cond.Statement
		if cond.Glue != "" {
			statement += " " + cond.Glue + " "
		}
	}
	return statement
}

func constructSelect(table string, columns []string, conditions []Condition) (statement string) {
	statement = fmt.Sprintf("SELECT %s FROM %s", strings.Join(columns, ", "), table)
	if conditions != nil {
		statement += " WHERE " + glueConditions(conditions)
	}
	Printer.Println(printssx.Loud, "Constructed statement:", statement)
	return
}

var countStr = []string{"COUNT(*)"}

// ForceFix - Completely useless data type meant to force various issues
type ForceFix struct {
}

func (db *Database) ExistsTable(tableName string) (bool, error) {
	statement := "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?"
	sqlRow, err := db.QueryRow(statement, db.name, tableName)
	if err != nil {
		return false, NewError("Table exists query", statement, err)
	}

	var count uint64
	err = sqlRow.Scan(&count)
	if err != nil {
		return false, NewError("Table exists scan", "", err)
	}

	return count == 1, nil
}

func (server *Server) ExistsTable(tableName string) (bool, error) {
	return server.dbPrimary.ExistsTable(tableName)
}

func (db *Database) Count(table string, conditions []Condition, args ...interface{}) (count uint64, err error) {
	statement := constructSelect(table, countStr, conditions)
	sqlRow, err := db.QueryRow(statement, args...)
	if err != nil {
		return
	}

	sqlRow.Scan(&count)
	return
}

func (server *Server) Count(table string, conditions []Condition, args ...interface{}) (count uint64, err error) {
	count, err = server.dbPrimary.Count(table, conditions, args...)
	return
}

func (db *Database) Select(table string, columns []string, conditions []Condition, args ...interface{}) (sqlRows *sql.Rows, count uint64, err error) {
	//statement := fmt.Sprintf("SELECT %%s FROM %s WHERE %s", table, conditi?%!(EXTRA string=ID, Command)ons)
	statement := constructSelect(table, columns, conditions)
	count, err = db.Count(table, conditions, args...)
	//fmt.Println(count)
	if err != nil || count == 0 {
		return
	}

	sqlRows, err = db.Query(statement, args...)
	return
}

func (server *Server) Select(table string, columns []string, conditions []Condition, args ...interface{}) (sqlRows *sql.Rows, count uint64, err error) {
	sqlRows, count, err = server.dbPrimary.Select(table, columns, conditions, args...)
	return
}

func (db *Database) SelectRow(table string, columns []string, conditions []Condition, args ...interface{}) (sqlRow *sql.Row, exists bool, err error) {
	//statement := fmt.Sprintf("SELECT %%s FROM %s WHERE %s", table, conditions)
	statement := constructSelect(table, columns, conditions)
	count, err := db.Count(table, conditions, args...)
	if err != nil || count == 0 {
		exists = false
		return
	}

	sqlRow, err = db.QueryRow(statement, args...)
	exists = true
	return
}

func (server *Server) SelectRow(table string, columns []string, conditions []Condition, args ...interface{}) (sqlRow *sql.Row, exists bool, err error) {
	sqlRow, exists, err = server.dbPrimary.SelectRow(table, columns, conditions, args...)
	return
}

/* Deprecating in favor of Select method
func (db *Database) Count(statement string, args ...interface{}) (count uint64, err error) {
	sqlRow, err := db.QueryRow(statement, args...)
	if err != nil {
		return
	}

	err = sqlRow.Scan(&count)
	return
}


func (server *Server) Count(statement string, args ...interface{}) (count uint64, err error) {
	count, err = server.dbPrimary.Count(statement, args...)
	return
}
*/

//Verify - Verifies the database exists
func (server *Server) Verify(dbName string) (verified bool, err error) {
	Printer.Println(printssx.Moderate, "Verifying database", dbName)

	//statement := "SELECT COUNT(*) FROM information_schema.schemata where schema_name = ?"
	count, err := server.Count("information_schema.schemata", []Condition{{Statement: "schema_name = ?"}}, dbName)
	if err != nil {
		return
	}

	if count == 0 {
		verified = false
	} else if count > 1 {
		err = NewError("DB Verify", "", Printer.Errorf("Invalid database count? [%d]", count))
		verified = false
	}
	return
}

// Connect - Connect to specific database
// create flag will create database if not exists
func (server *Server) Connect(dbName string, user string, pass []byte, create bool) (db *Database, err error) {
	Printer.Printf(printssx.Subtle, "Connecting to %s:%s:%s with user %s\n", server.name, server.port, dbName, user)

	operation := "DB Connect"
	if server.dbPrimary == nil {
		err = NewError(operation, "", Printer.Errorf("Primary Database not set"))
		return
	} else {
		database := server.DatabaseByName(dbName)
		if database != nil {
			return database, nil
		}
	}

	var verified bool
	verified, err = server.Verify(dbName)
	if err != nil {
		return
	}

	if !verified {
		if create {
			_, err = server.Execute(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName))
			if err != nil {
				return
			}
		} else {
			err = NewError(operation, "", fmt.Errorf("Database [%s] not found", dbName))
			return
		}
	}
	db, err = Open(server.name, server.port, dbName, user, pass)
	return
}

/*
//PrimaryConnect - Connect primary database for all logistical queries
func PrimaryConnect(server, port, dbName, username, password string) (dbOut *Database, err error) {
	Printer.Printf(printssx.Subtle, "Connecting to PRIMARY %s:%s:%s with user %s\n", server, port, dbName, username)
	sqlDB, err := Open(server, port, dbName, username, password)
	if err != nil {
		return
	}
	dbPrimaryName = dbName
	dbCatalog[server] = make(map[string]*Database)
	dbCatalog[server][dbName] = sqlDB
	dbPrimary = sqlDB
	dbOut = sqlDB
	return
}
*/

//TableNameGuide - No
type TableNameGuide struct {
	Glue     string
	Pre      string
	Override string
	Post     string
	Plural   bool
}

//GetName - Self explanatory
func (tng *TableNameGuide) GetName(inName string) (outName string) {
	if len(tng.Override) > 0 {
		outName = tng.Override
	} else {
		outName = inName
	}
	if tng.Plural {
		outName = outName + "s"
	}
	if len(tng.Pre) > 0 {
		outName = tng.Pre + tng.Glue + outName
	}
	if len(tng.Post) > 0 {
		outName = outName + tng.Glue + tng.Post
	}
	return
}

//InitTable - Initializes a table for the provided database per a struct type
//Kudos to Fatih's structs library
func (db *Database) InitTable(v interface{}, tng *TableNameGuide) (err error) {
	//Get the name of the table

	var tableName = structs.Name(v)
	if tng != nil {
		tableName = tng.GetName(tableName)
	}
	Printer.Printf(printssx.Subtle, "Initializing %s/%s", db.name, tableName)
	//fmt.Printf("Initializing database [%s] with table [%s]\n", databaseName, tableName)

	//fieldNames := structs.Names(v)
	fields := structs.Fields(v)
	namesToFields := make(map[string]*structs.Field)

	for _, field := range fields {
		namesToFields[field.Name()] = field
	}

	columns := make([]string, len(fields))
	for i := range columns {
		columns[i] = fields[i].Name() + " " + fields[i].Tag("sql")
	}

	query := "CREATE TABLE IF NOT EXISTS `" + tableName + "` (" + strings.Join(columns, ", ") + ")"
	_, err = db.Execute(query)
	if err != nil {
		return
	}

	var columnName string
	var columnNames []string
	query = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? and TABLE_NAME = ?"
	//ssql.Execute(query)
	sqlRows, err := db.Query(query, db.name, tableName)
	if err != nil {
		return
	}
	for sqlRows.Next() {
		sqlRows.Scan(&columnName)
		columnNames = append(columnNames, columnName)
	}
	sqlRows.Close()

	sqlRenames := make(map[string]string)
	for fieldName, field := range namesToFields {
		newName := field.Tag("sqlRename")
		if len(newName) == 0 {
			continue
		}
		sqlRenames[newName] = fieldName
	}

	//Rename columns
	for _, columnName := range columnNames {
		//fmt.Printf("Analyzing [rename] database [%s] table [%s] field [%s]\n", dbName, tableName, columnName)
		newName, ok := sqlRenames[columnName]
		if !ok {
			continue
		}
		//fmt.Printf("Renaming [%s][%s][%s] to [%s][%s][%s]\n", databaseName, tableName, columnName, databaseName, tableName, newName)
		statement := fmt.Sprintf("ALTER TABLE `%s` CHANGE COLUMN %s %s %s", tableName, columnName, newName, namesToFields[newName].Tag("sql"))
		_, err = db.Execute(statement)
		if err != nil {
			return
		}
		/*
			newName := namesToFields[columnName].Tag("sqlRenameFrom")
			fmt.Printf("New name [%s]\n", newName)
			if len(newName) == 0 {
				continue
			}
			if columnName != newName {
				log.Printf("Renaming [%s] to [%s]\n", columnName, newName)
				continue
				log.Println(columnName, "dropping from", tableName)
				query = "ALTER TABLE " + tableName + " DROP COLUMN " + columnName
				_, err := dbExecute(sqlDB, query)
				uPanic(err)
			}
		*/
	}

	// Remove columns
	/* Removing for safety/security reasons
	for _, columnName = range columnNames {
		_, ok := namesToFields[columnName]
		if !ok {
			log.Println(columnName, "dropping from", tableName)
			query = "ALTER TABLE " + tableName + " DROP COLUMN " + columnName
			_, err := DatabaseExecute(sqlDB, query)
			uPanic(err)
		}
	}
	*/

	// Add columns
	for _, columnName = range structs.Names(v) {
		if !extdata.StringArrayContains(columnNames, columnName) {
			Printer.Println(printssx.Subtle, "Adding column", columnName, namesToFields[columnName].Tag("sql"))
			//log.Println(columnName, "adding to", tableName)
			query = "ALTER TABLE `" + tableName + "` ADD COLUMN " + columnName + " " + namesToFields[columnName].Tag("sql")
			_, err = db.Execute(query)
			if err != nil {
				return
			}
		} else {
			if modify := namesToFields[columnName].Tag("sqlModify"); modify == "true" {
				Printer.Println(printssx.Subtle, "Modfying column", columnName, namesToFields[columnName].Tag("sql"))
				query = "ALTER TABLE `" + tableName + "` MODIFY " + columnName + " " + namesToFields[columnName].Tag("sql")
				_, err = db.Execute(query)
				if err != nil {
					return
				}
			}
		}
	}
	return
}

func (server *Server) InitTable(v interface{}, tng *TableNameGuide) (err error) {
	err = server.dbPrimary.InitTable(v, tng)
	return
}

func (server *Server) Close() {
	Printer.Printf(printssx.Subtle, "Closing server %s/%s\n", server.name, server.port)
	for _, db := range server.dbCatalog {
		Printer.Printf(printssx.Subtle, "Closing database %s\n", db.name)
		db.sqlDB.Close()
	}
}

//Close - No
func Close() {
	for _, server := range serverCatalog {
		server.Close()
	}

}
