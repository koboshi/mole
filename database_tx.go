package mole

import (
	"database/sql"
	"fmt"
	"strings"
)

// 数据库事务
type DatabaseTx struct {
	tx *sql.Tx
}

// 事务提交
func (databaseTx *DatabaseTx) Commit() (error) {
	return databaseTx.tx.Commit()
}

// 事务回滚
func (databaseTx *DatabaseTx) Rollback() (error) {
	return databaseTx.tx.Rollback()
}

// 执行查询，返回多行数据集
func (databaseTx *DatabaseTx) Query (sql string, args ...interface{}) (*sql.Rows, error) {
	rows, err := databaseTx.tx.Query(sql, args...)
	return rows, err
}

// 执行查询，返回单个数据集
func (databaseTx *DatabaseTx) QueryOne(sql string, args ...interface{}) (*sql.Row) {
	return databaseTx.tx.QueryRow(sql, args...)
}

// 以事务方式，新增数据至mysql
// 要新增的数据以map形式传入
// 返回sql.Result以及error
func (databaseTx *DatabaseTx) Insert(data map[string]interface{}, tblName string) (int64, error) {
	return databaseTx.internalInsert(data, tblName, "INSERT")
}

// 以事务方式，以INSERT IGNORE INTO形式新增数据至mysql
// 要新增的数据以map形式传入
// 返回sql.Result以及error
func (databaseTx *DatabaseTx) Ignore(data map[string]interface{}, tblName string) (int64, error) {
	return databaseTx.internalInsert(data, tblName, "INSERT IGNORE")
}

// 以事务方式，以REPLACE INTO形式新增数据至mysql
// 要新增的数据以map形式传入
// 返回sql.Result以及error
func (databaseTx *DatabaseTx) Replace(data map[string]interface{}, tblName string) (int64, error) {
	return databaseTx.internalInsert(data, tblName, "REPLACE")
}

// 以事务方式更新mysql数据
// 要更新的数据以map形式传入
// UPDATE的WHERE语句以字符串形式传入，支持传入where语句参数，占位符为 ? ,会自动转义
// 返回sql.Result以及error
func (databaseTx *DatabaseTx) Update(data map[string]interface{}, tblName string, whereStr string, whereArgs ...interface{}) (int64, error) {
	fields := make([]string, 0, 10)
	values := make([]interface{}, 0, 10)
	for field, value := range data {
		var subField = fmt.Sprintf("`%s` = ?", field)
		fields = append(fields, subField)
		values = append(values, value)
	}
	setStr := strings.Join(fields, ", ")
	updateSql := fmt.Sprintf("UPDATE %s SET %s WHERE %s", tblName, setStr, whereStr)
	if len(whereArgs) > 0 {
		values = append(values, whereArgs...)
	}
	result, err := databaseTx.tx.Exec(updateSql, values...)
	if err != nil {
		panic(err)
	}
	var affectedRows int64
	affectedRows, _ = result.RowsAffected()
	return affectedRows, err
}

// 以事务方式删除mysql数据
// DELETE FROM的WHERE语句以字符串形式传入，支持传入where语句参数，占位符为 ? ,会自动转义
// 返回sql.Result以及error
func (databaseTx *DatabaseTx) Delete(tblName string, whereStr string, whereArgs ...interface{}) (int64, error) {
	deleteSql := fmt.Sprintf("DELETE FROM %s WHERE %s", tblName, whereStr)
	result, err := databaseTx.tx.Exec(deleteSql, whereArgs...)
	if err != nil {
		panic(err)
	}
	var affectedRows int64
	affectedRows, _ = result.RowsAffected()
	return affectedRows, err
}

// 执行数据写入的内部方法,支持insert/insert ignore/replace
func (databaseTx *DatabaseTx) internalInsert( data map[string]interface{}, tblName string, insertType string) (int64, error) {
	fields := make([]string, 0, 10)
	values := make([]interface{}, 0, 10)
	var subField string
	for field, value := range data {
		subField = fmt.Sprintf("`%s` = ?", field)
		fields = append(fields, subField)
		values = append(values, value)
	}
	setStr := strings.Join(fields, ", ")
	insertSql := fmt.Sprintf("%s INTO %s SET %s", insertType, tblName, setStr)
	result, err := databaseTx.tx.Exec(insertSql, values...)
	if err != nil {
		panic(err)
	}
	var lastInsertId int64
	lastInsertId, _ = result.LastInsertId()
	return lastInsertId, err
}