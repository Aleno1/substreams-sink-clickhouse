package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_prepareColValues(t *testing.T) {
	type args struct {
		table     *TableInfo
		colValues map[string]string
	}
	tests := []struct {
		name        string
		args        args
		wantColumns []string
		wantValues  []string
		assertion   require.ErrorAssertionFunc
	}{
		{
			"bool true",
			args{
				newTable(t, "schema", "name", "id", NewColumnInfo("col", "bool", true)),
				map[string]string{"col": "true"},
			},
			[]string{`"col"`},
			[]string{`'true'`},
			require.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotColumns, gotValues, err := prepareColValues(tt.args.table, tt.args.colValues)
			tt.assertion(t, err)
			assert.Equal(t, tt.wantColumns, gotColumns)
			assert.Equal(t, tt.wantValues, gotValues)
		})
	}
}

func newTable(t *testing.T, schema, name, primaryColumn string, columnInfos ...*ColumnInfo) *TableInfo {
	columns := make(map[string]*ColumnInfo)
	columns[primaryColumn] = NewColumnInfo(primaryColumn, "text", "")
	for _, columnInfo := range columnInfos {
		columns[columnInfo.name] = columnInfo
	}

	table, err := NewTableInfo("public", "data", "id", columns)
	require.NoError(t, err)

	return table
}
