package db

import (
	_ "github.com/lib/pq"
	"github.com/streamingfast/logging"
)

var zlog, tracer = logging.PackageLogger("sink-clickhouse", "https://github.com/aleno-ai/substreams-sink-clickhouse/db")

func init() {
	logging.InstantiateLoggers()
}
