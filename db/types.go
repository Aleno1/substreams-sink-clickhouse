package db

import (
	"fmt"
	"reflect"
)

//go:generate go-enum -f=$GOFILE --marshal --names -nocase

// ENUM(
//
//	 Ignore
//		Warn
//		Error
//
// )
type OnModuleHashMismatch uint

type TableInfo struct {
	name          string
	nameEscaped   string
	columnsByName map[string]*ColumnInfo
	primaryColumn *ColumnInfo

	// Identifier is equivalent to 'escape(<schema>).escape(<name>)' but pre-computed
	// for usage when computing queries.
	identifier string
}

func NewTableInfo(name, primaryKeyColumnName string, columnsByName map[string]*ColumnInfo) (*TableInfo, error) {
	nameEscaped := escapeIdentifier(name)
	primaryColumn, found := columnsByName[primaryKeyColumnName]
	if !found {
		return nil, fmt.Errorf("primary key column %q not found", primaryKeyColumnName)
	}

	return &TableInfo{
		name:          name,
		nameEscaped:   nameEscaped,
		identifier:    nameEscaped,
		primaryColumn: primaryColumn,
		columnsByName: columnsByName,
	}, nil
}

type ColumnInfo struct {
	name             string
	escapedName      string
	databaseTypeName string
	scanType         reflect.Type
}

func NewColumnInfo(name string, databaseTypeName string, scanType any) *ColumnInfo {
	return &ColumnInfo{
		name:             name,
		escapedName:      escapeIdentifier(name),
		databaseTypeName: databaseTypeName,
		scanType:         reflect.TypeOf(scanType),
	}
}
