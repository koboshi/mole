package mole

import (
	"fmt"
	"strings"
)

func buildInsertSql (data map[string]interface{}, tblName string, insertType string) (patternSql string, patternValues []interface{}) {
	fields := make([]string, 0)
	values := make([]interface{}, 0)
	var subField string
	for field, value := range data {
		subField = fmt.Sprintf("`%s` = ?", field)
		fields = append(fields, subField)
		values = append(values, value)
	}
	setStr := strings.Join(fields, ", ")
	insertSql := fmt.Sprintf("%s INTO %s SET %s", insertType, tblName, setStr)

	return insertSql, values
}

func buildUpdateSql (data map[string]interface{}, tblName string, whereStr string, whereArgs ...interface{}) (patternSql string, patternValues []interface{}) {
	fields := make([]string, 0)
	values := make([]interface{}, 0)
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

	return updateSql, values
}