package sqlssx

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/Stachio/go-extdata"
	"github.com/Stachio/go-printssx"
	"github.com/fatih/structs"

	//Mysql driver
	_ "github.com/go-sql-driver/mysql"
)

//Printer - Generic printer object provided by stachio/printerssx
var Printer = printssx.New("SQLSSX", log.Println, log.Printf, printssx.Subtle, printssx.Subtle)

//Database - Database struct used to encapsulate sqlssx functinos
type Database struct {
	name  string
	sqlDB *sql.DB
}

//GetName - Get function to protect name value
func (db *Database) GetName() string {
	return db.name
}

//Server - Server struct for multiple databases on a server
type Server struct {
	name string
	port string
	user string
	pass []rune

	dbCatalog     map[string]*Database //= make(map[string]*Database)
	dbPrimaryName string
	dbPrimary     *Database
}

var serverCatalog = make(map[string]*Server)

//Open - Open a pre-built database
//Note: database MUST exists
func Open(server, port, dbName, user string, pass []rune) (db *Database, err error) {
	Printer.Printf(printssx.Moderate, "Opening database %s/%s\n", server, dbName)
	sqlDB, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, string(pass), server, port, dbName))
	if err != nil {
		return
	}

	// Pretty sure we need to do a simple query to ensure the db is connected
	err = sqlDB.Ping()
	if err != nil {
		return
	}

	sqlDB.SetMaxIdleConns(0)
	db = &Database{name: dbName, sqlDB: sqlDB}
	//dbCatalog[server] = make(map[string]*Database)
	//dbCatalog[server][dbName] = db
	return
}

func NewServer(name, port, dbName, user string, pass []rune) (server *Server, err error) {
	Printer.Printf(printssx.Subtle, "Connecting to PRIMARY %s:%s:%s with user %s\n", name, port, dbName, user)
	db, err := Open(name, port, dbName, user, pass)
	if err != nil {
		return
	}

	server = &Server{
		name:          name,
		port:          port,
		user:          user,
		pass:          pass,
		dbPrimary:     db,
		dbPrimaryName: dbName,
		dbCatalog:     make(map[string]*Database),
	}
	server.dbCatalog[dbName] = db
	serverCatalog[name] = server
	return
}

//GetName - Get function to protect name value
func (server *Server) GetName() string {
	return server.name
}

//Execute - Excecute a statement against the associated database
func (db *Database) Execute(statement string, args ...interface{}) (out *sql.Result, err error) {
	Printer.Println(printssx.Loud, "Executing", statement)
	sqlStatement, err := db.sqlDB.Prepare(statement)
	if err != nil {
		return
	}

	sqlResult, err := sqlStatement.Exec(args...)
	sqlStatement.Close()
	out = &sqlResult
	return
}

func (server *Server) Execute(statement string, args ...interface{}) (result *sql.Result, err error) {
	result, err = server.dbPrimary.Execute(statement, args...)
	return
}

//Query - Query a statement against the associated database
//Returns *sql.Rows
func (db *Database) Query(statement string, args ...interface{}) (sqlRows *sql.Rows, err error) {
	Printer.Println(printssx.Loud, "Querying", statement)
	sqlStatement, err := db.sqlDB.Prepare(statement)
	if err != nil {
		return
	}

	sqlRows, err = sqlStatement.Query(args...)
	sqlStatement.Close()
	return
}

func (server *Server) Query(statement string, args ...interface{}) (sqlRows *sql.Rows, err error) {
	sqlRows, err = server.dbPrimary.Query(statement, args...)
	return
}

//QueryRow - Query a statement against the provided database
//Returns *sql.Row
func (db *Database) QueryRow(statement string, args ...interface{}) (sqlRow *sql.Row, err error) {
	Printer.Println(printssx.Loud, "Single query", statement)
	sqlStatement, err := db.sqlDB.Prepare(statement)
	if err != nil {
		return
	}

	sqlRow = sqlStatement.QueryRow(args...)
	sqlStatement.Close()
	return
}

func (server *Server) QueryRow(statement string, args ...interface{}) (sqlRow *sql.Row, err error) {
	sqlRow, err = server.dbPrimary.QueryRow(statement, args...)
	return
}

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

//Verify - Verifies the database exists
func (server *Server) Verify(dbName string) (verified bool, err error) {
	Printer.Println(printssx.Moderate, "Verifying database", dbName)

	statement := "SELECT COUNT(*) FROM information_schema.schemata where schema_name = ?"
	count, err := server.Count(statement, dbName)
	if err != nil {
		return
	}

	if count == 0 {
		verified = false
	} else if count > 1 {
		err = Printer.Errorf("Invalid database count? [%d]", count)
		verified = false
	}
	return
}

//Connect - Connect to specific database
//create flag will create database if not exists
func (server *Server) Connect(dbName string, create bool) (db *Database, err error) {
	Printer.Printf(printssx.Subtle, "Connecting to %s:%s:%s with user %s\n", server.name, server.port, dbName, server.user)

	if server.dbPrimary == nil {
		err = Printer.Errorf("Primary Database not set")
		return
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
			err = fmt.Errorf("Database [%s] not found", dbName)
			return
		}
	}
	db, err = Open(server.name, server.port, dbName, server.user, server.pass)
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
			//log.Println(columnName, "adding to", tableName)
			query = "ALTER TABLE `" + tableName + "` ADD COLUMN " + columnName + " " + namesToFields[columnName].Tag("sql")
			_, err = db.Execute(query)
			if err != nil {
				return
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
