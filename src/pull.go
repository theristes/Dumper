package dumper

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

type Pull[T interface{}] struct {
	DB    *sql.DB
	Query string
	Args  []any
}

// type FieldName string | any

func (p *Pull[T]) GetColumns() ([]string, error) {

	rows, err := p.DB.Query(p.Query, p.Args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return rows.Columns()

}

func (p *Pull[T]) Run() (Data []T, Error error) {

	rows, err := p.DB.Query(p.Query, p.Args...)
	if err != nil {
		Error = err
		return
	}
	defer rows.Close()

	dbColumns, err := rows.Columns()
	if err != nil {
		Error = err
		return
	}

	if len(dbColumns) == 0 {
		Error = errors.New("no columns found")
		return
	}

	dbCellValues := make([]any, len(dbColumns))
	dbCellPointers := make([]any, len(dbColumns))
	for i := range dbCellValues {
		dbCellPointers[i] = &dbCellValues[i]
	}

	for rows.Next() {

		err = rows.Scan(dbCellPointers...)
		if err != nil {
			Error = err
			return
		}

		var Obj *T
		objType := reflect.TypeOf(Obj)
		if objType == nil {
			Error = errors.New("obj is nil")
			return
		}

		if objType.Kind() != reflect.Ptr {
			Error = errors.New("obj is not a pointer")
			return
		}

		reflectObject := reflect.New(reflect.TypeOf(Obj).Elem()).Elem()

		for i, dbCellValue := range dbCellValues {

			columnName := dbColumns[i]
			fieldName := getFieldNameByColumnName(reflectObject, columnName)
			handleFields(reflectObject, fieldName, dbCellValue)

		}
		Data = append(Data, reflectObject.Interface().(T))
	}

	return Data, nil
}

func handleFields(reflectObject reflect.Value, fieldName string, dbCellValue any) {

	if dbCellValue == nil {
		return
	}

	if fieldName == "" {
		return
	}

	switch reflectObject.FieldByName(fieldName).Kind() {

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			str := fmt.Sprintf("%v", dbCellValue)
			iVal, _ := strconv.Atoi(str)
			switch reflectObject.FieldByName(fieldName).Kind() {
			case reflect.Int64:
				reflectObject.FieldByName(fieldName).SetInt(int64(iVal))
			case reflect.Int32:
				reflectObject.FieldByName(fieldName).SetInt(int64(int32(iVal)))
			case reflect.Int16:
				reflectObject.FieldByName(fieldName).SetInt(int64(int16(iVal)))
			case reflect.Int8:
				reflectObject.FieldByName(fieldName).SetInt(int64(int8(iVal)))
			case reflect.Int:
				reflectObject.FieldByName(fieldName).SetInt(int64(iVal))
			}

		case reflect.String:
			refType := reflect.TypeOf(dbCellValue).String()
			if refType == "float64" || refType == "float32" {
				str := fmt.Sprintf("%v", dbCellValue)
				reflectObject.FieldByName(fieldName).SetString(str) 
			} else if refType == "int" || refType == "int8" || refType == "int16" || refType == "int32" || refType == "int64" {
				str := fmt.Sprintf("%v", dbCellValue)
				reflectObject.FieldByName(fieldName).SetString(str)
			} else if refType == "[]uint8" {
				reflectObject.FieldByName(fieldName).SetString(string(dbCellValue.([]byte)))
			} else {
				reflectObject.FieldByName(fieldName).SetString(dbCellValue.(string))
			}

		case reflect.Float32:
			if reflect.TypeOf(dbCellValue).String() != "float32" {
				str := fmt.Sprintf("%v", dbCellValue)
				fVal, _ := strconv.ParseFloat(str, 32)
				reflectObject.FieldByName(fieldName).SetFloat(float64(float32(fVal)))
			} else {
				reflectObject.FieldByName(fieldName).SetFloat(float64(dbCellValue.(float32)))
			}

		case reflect.Float64:
			if reflect.TypeOf(dbCellValue).String() != "float64" {
				str := fmt.Sprintf("%v", dbCellValue)
				fVal, _ := strconv.ParseFloat(str, 64)
				reflectObject.FieldByName(fieldName).SetFloat(fVal)
			} else {
				reflectObject.FieldByName(fieldName).SetFloat(dbCellValue.(float64))
			}

		case reflect.Bool:
			reflectObject.FieldByName(fieldName).SetBool(dbCellValue.(bool))

		case reflect.Slice:
			reflectObject.FieldByName(fieldName).SetBytes(dbCellValue.([]byte))
			
		case reflect.Struct:
			if reflectObject.FieldByName(fieldName).Type().String() == "time.Time" {
				if reflect.TypeOf(dbCellValue).String() == "[]uint8" {
					timeStr := string(dbCellValue.([]uint8))
					parsedTime, _ := time.Parse("2006-01-02 15:04:05", timeStr)
					reflectObject.FieldByName(fieldName).Set(reflect.ValueOf(parsedTime))
					return
				}
				reflectObject.FieldByName(fieldName).Set(reflect.ValueOf(dbCellValue.(time.Time)))
			}
		}
}

func getFieldNameByColumnName(reflectObject reflect.Value, columnName string) string {
	for i := 0; i < reflectObject.NumField(); i++ {
		if reflectObject.Type().Field(i).Tag.Get("field") == columnName {
			return reflectObject.Type().Field(i).Name
		}
	}
	return ""
}
