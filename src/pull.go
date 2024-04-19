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
			// Handle error: Obj is nil
			return
		}

		if objType.Kind() != reflect.Ptr {
			// Handle error: Obj is not a pointer
			return
		}

		reflectObject := reflect.New(reflect.TypeOf(Obj).Elem()).Elem()


		for i, dbCellValue := range dbCellValues {

			columnName := dbColumns[i]
			fieldName := getFieldNameByColumnName(reflectObject, columnName)

			// get the fieldName by the columnName
			if dbCellValue == nil {
				continue
			}

			if fieldName == "" {
				continue
			}
		
			var val any
			// handling the values types
			if (reflectObject.FieldByName(fieldName).Kind() ==  reflect.Int64 ||
				reflectObject.FieldByName(fieldName).Kind() ==  reflect.Int32 ||
				reflectObject.FieldByName(fieldName).Kind() ==  reflect.Int16 ||
				reflectObject.FieldByName(fieldName).Kind() ==  reflect.Int8 ||
				reflectObject.FieldByName(fieldName).Kind() ==  reflect.Int){

				str :=  fmt.Sprintf("%v",dbCellValue)
				iVal,_ := strconv.Atoi(str)
				switch reflectObject.FieldByName(fieldName).Kind() {
				case reflect.Int64:
					val = int64(iVal)
				case reflect.Int32:
					val = int32(iVal)
				case reflect.Int16:
					val = int16(iVal)
				case reflect.Int8:
					val = int8(iVal)
				case reflect.Int:
					val = iVal
				}

			} else if reflectObject.FieldByName(fieldName).Kind() ==  reflect.String {
				// check the type of value
				if reflect.TypeOf(dbCellValue).String() != "string" {
					val = fmt.Sprintf("%v",dbCellValue)
				} else {
					val = dbCellValue.(string)
				}
			} else if reflectObject.FieldByName(fieldName).Kind() ==  reflect.Float32 {

				if reflect.TypeOf(dbCellValue).String() != "float32" {
					str :=  fmt.Sprintf("%v",dbCellValue)
					fVal,_ := strconv.ParseFloat(str, 32)
					val = float32(fVal)
				} else {
					val = dbCellValue.(float32)
				}
			} else if reflectObject.FieldByName(fieldName).Kind() ==  reflect.Float64 {
				if reflect.TypeOf(dbCellValue).String() != "float64" {
					str :=  fmt.Sprintf("%v",dbCellValue)
					fVal,_ := strconv.ParseFloat(str, 64)
					val = fVal
				} else {
					val = dbCellValue.(float64)
				}
			} else if reflectObject.FieldByName(fieldName).Kind() ==  reflect.Bool {
				val = dbCellValue.(bool)
			} else if reflectObject.FieldByName(fieldName).Kind() ==  reflect.Slice {
				val = dbCellValue.([]byte)
			} else if reflectObject.FieldByName(fieldName).Kind() ==  reflect.Struct {
				if reflectObject.FieldByName(fieldName).Type().String() == "time.Time" {
					val = dbCellValue.(time.Time)
				} else {
					continue
				}
			} else {
				continue
			}

			reflectObject.FieldByName(fieldName).Set(reflect.ValueOf(val))
		
		}
		Data = append(Data, reflectObject.Interface().(T))
	}

	return Data, nil
}


func getFieldNameByColumnName(reflectObject reflect.Value, columnName string) string {
	for i := 0; i < reflectObject.NumField(); i++ {
		if reflectObject.Type().Field(i).Tag.Get("field") == columnName {
			return reflectObject.Type().Field(i).Name
		}
	}
	return ""
}
