package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDSN(t *testing.T) {
	tests := []struct {
		name        string
		dns         string
		expectError bool
		expectDSN   *DSN
	}{
		{
			name: "golden path",
			dns:  "clickhouse://username:password@host:8888/database",
			expectDSN: &DSN{
				original: "clickhouse://username:password@host:8888/database",
				host:     "host",
				port:     8888,
				username: "username",
				password: "password",
				database: "database",
				options:  []string{},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.dns, func(t *testing.T) {
			d, err := parseDSN(test.dns)
			if test.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectDSN, d)
			}
		})
	}

}
