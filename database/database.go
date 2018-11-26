package database

import (
	"database/sql"
	"github.com/go-sql-driver/mysql"
	"fmt"
	"time"
)

type Database struct {
	db *sql.DB
}

func New(host string, username string, password string, schema string, charset string) (*Database, error) {
	db := new(Database)
	customParams := make(map[string]string)
	customParams["readTimeout"] = "30m"
	customParams["writeTimeout"] = "30m"
	err := db.Connect(host, username, password, schema, charset, customParams)
	if err == nil {
		return nil, err
	}
	return db, nil
}

func (database *Database) Connect (host string, username string, password string, schema string, charset string, customParams map[string]string) error {
	//DSN: [username[:password]@][protocol[(address)]]/schema[?param1=value1&...&paramN=valueN]
	//初始化DSN参数
	params := make(map[string]string)
	params["charset"] = charset
	//默认参数，可以用通过customParams重新设置
	params["parseTime"] = "true"
	params["readTimeout"] = "30m"
	params["writeTimeout"] = "1m"
	params["allowNativePasswords"] = "true"
	for param, value := range customParams {
		params[param] = value
	}
	config := new(mysql.Config)//通过mysql.Config创建dsn
	config.Addr = host
	config.User = username
	config.Passwd = password
	config.DBName = schema
	config.Params = params
	//发起数据库连接(lazy connect)
	var err error
	database.db, err = sql.Open("mysql", config.FormatDSN())
	return err
}

// 关闭数据库链接
func (database *Database) Close() (error) {
	return database.db.Close()
}

//开始事务，返回DatabaseTx
func (database *Database) Begin() (*DatabaseTx, error) {
	databaseTx := new(DatabaseTx)
	var err error
	databaseTx.tx, err = database.db.Begin()
	if err != nil {
		panic(err)
	}
	return databaseTx, err
}

// 设置连接池配置参数
// 最大打开链接数
// 最大空闲链接
// 链接重用次数 <=0永久重用
func (database *Database) SetPool(maxOpenConns int, maxIdleConns int, connMaxLifetime time.Duration) {
	database.db.SetMaxOpenConns(maxOpenConns)
	database.db.SetMaxIdleConns(maxIdleConns)
	database.db.SetConnMaxLifetime(connMaxLifetime)
}

// 执行查询，返回多行数据集
func (database *Database) Query (sql string, args ...interface{}) (*sql.Rows, error) {
	rows, err := database.db.Query(sql, args...)
	return rows, err
}

// 执行查询，返回单行数据集
func (database *Database) QueryOne(sql string, args ...interface{}) (*sql.Row) {
	return database.db.QueryRow(sql, args...)
}

// 新增数据至mysql
// 要新增的数据以map形式传入
// 返回sql.Result以及error
func (database *Database) Insert(data map[string]interface{}, tblName string) (int64, error) {
	return database.internalInsert(data, tblName, "INSERT")
}

// 以INSERT IGNORE INTO形式新增数据至mysql
// 要新增的数据以map形式传入
// 返回sql.Result以及error
func (database *Database) Ignore(data map[string]interface{}, tblName string) (int64, error) {
	return database.internalInsert(data, tblName, "INSERT IGNORE")
}

// 以REPLACE INTO形式新增数据至mysql
// 要新增的数据以map形式传入
// 返回sql.Result以及error
func (database *Database) Replace(data map[string]interface{}, tblName string) (int64, error) {
	return database.internalInsert(data, tblName, "REPLACE")
}

// 更新mysql数据
// 要更新的数据以map形式传入
// UPDATE的WHERE语句以字符串形式传入，支持传入where语句参数，占位符为 ? ,会自动转义
// 返回sql.Result以及error
func (database *Database) Update(data map[string]interface{}, tblName string, whereStr string, whereArgs ...interface{}) (int64, error) {
	updateSql, values := buildUpdateSql(data, tblName, whereStr, whereArgs...)
	result, err := database.db.Exec(updateSql, values...)
	if err != nil {
		return 0, err
	}
	var affectedRows int64
	affectedRows, _ = result.RowsAffected()
	return affectedRows, err
}

// 删除mysql数据
// DELETE FROM的WHERE语句以字符串形式传入，支持传入where语句参数，占位符为 ? ,会自动转义
// 返回sql.Result以及error
func (database *Database) Delete(tblName string, whereStr string, whereArgs ...interface{}) (int64, error) {
	deleteSql := fmt.Sprintf("DELETE FROM %s WHERE %s", tblName, whereStr)
	result, err := database.db.Exec(deleteSql, whereArgs...)
	if err != nil {
		return 0, err
	}
	var affectedRows int64
	affectedRows, _ = result.RowsAffected()
	return affectedRows, err
}

// 执行数据写入的内部方法,支持insert/insert ignore/replace
func (database *Database) internalInsert (data map[string]interface{}, tblName string, insertType string) (int64, error) {
	insertSql, values := buildInsertSql(data, tblName, insertType)
	result, err := database.db.Exec(insertSql, values...)
	if err != nil {
		return 0, err
	}
	var lastInsertId int64
	lastInsertId, _ = result.LastInsertId()
	return lastInsertId, err
}