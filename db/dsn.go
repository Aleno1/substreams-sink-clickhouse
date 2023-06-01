package db

import (
	"fmt"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/drone/envsubst"
)

type DSN struct {
	original string

	host     string
	port     int64
	username string
	password string
	database string
	options  []string
}

func parseDSN(dsn string) (*DSN, error) {
	expanded, err := envsubst.Eval(dsn, os.Getenv)
	if err != nil {
		return nil, fmt.Errorf("variables expansion failed: %w", err)
	}

	dsnURL, err := url.Parse(expanded)
	if err != nil {
		return nil, fmt.Errorf("invalid url: %w", err)
	}

	if dsnURL.Scheme != "clickhouse" {
		return nil, fmt.Errorf(`invalid scheme %q, should be "clickhouse"`, dsnURL.Scheme)
	}

	host := dsnURL.Hostname()

	port := int64(5432)
	if strings.Contains(dsnURL.Host, ":") {
		port, _ = strconv.ParseInt(dsnURL.Port(), 10, 32)
	}

	username := dsnURL.User.Username()
	password, _ := dsnURL.User.Password()
	database := strings.TrimPrefix(dsnURL.EscapedPath(), "/")

	query := dsnURL.Query()
	keys := make([]string, 0, len(query))
	for key := range dsnURL.Query() {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	d := &DSN{
		original: dsn,
		host:     host,
		port:     port,
		username: username,
		password: password,
		database: database,
	}

	d.options = make([]string, len(query))
	for i, key := range keys {
		d.options[i] = fmt.Sprintf("%s=%s", key, strings.Join(query[key], ","))
	}

	return d, nil
}
