package sinker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aleno-ai/substreams-sink-clickhouse/db"
	pbddatabase "github.com/aleno-ai/substreams-sink-clickhouse/pb/substreams/sink/database/v1"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	sink "github.com/streamingfast/substreams-sink"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const (
	HISTORICAL_BLOCK_FLUSH_EACH = 1000
	LIVE_BLOCK_FLUSH_EACH       = 1
)

type ClickhouseSinker struct {
	*shutter.Shutter
	*sink.Sinker

	loader *db.Loader
	logger *zap.Logger
	tracer logging.Tracer

	stats *Stats
}

func New(sink *sink.Sinker, loader *db.Loader, logger *zap.Logger, tracer logging.Tracer) (*ClickhouseSinker, error) {
	return &ClickhouseSinker{
		Shutter: shutter.New(),
		Sinker:  sink,

		loader: loader,
		logger: logger,
		tracer: tracer,

		stats: NewStats(logger),
	}, nil
}

func (s *ClickhouseSinker) Run(ctx context.Context) {
	cursor, mistmatchDetected, err := s.loader.GetCursor(ctx, s.OutputModuleHash())
	if err != nil && !errors.Is(err, db.ErrCursorNotFound) {
		s.Shutdown(fmt.Errorf("unable to retrieve cursor: %w", err))
		return
	}

	// We write an empty cursor right away in the database because the flush logic
	// only performs an `update` operation so an initial cursor is required in the database
	// for the flush to work correctly.
	if errors.Is(err, db.ErrCursorNotFound) {
		if err := s.loader.InsertCursor(ctx, s.OutputModuleHash(), sink.NewBlankCursor()); err != nil {
			s.Shutdown(fmt.Errorf("unable to write initial empty cursor: %w", err))
			return
		}
	} else if mistmatchDetected {
		if err := s.loader.InsertCursor(ctx, s.OutputModuleHash(), cursor); err != nil {
			s.Shutdown(fmt.Errorf("unable to write new cursor after module mistmatch: %w", err))
			return
		}
	}

	s.Sinker.OnTerminating(s.Shutdown)
	s.OnTerminating(func(err error) {
		s.stats.LogNow()
		s.logger.Info("clickhouse sinker terminating", zap.Stringer("last_block_written", s.stats.lastBlock))
		s.Sinker.Shutdown(err)
	})

	s.OnTerminating(func(_ error) { s.stats.Close() })
	s.stats.OnTerminated(func(err error) { s.Shutdown(err) })

	logEach := 15 * time.Second
	if s.logger.Core().Enabled(zap.DebugLevel) {
		logEach = 5 * time.Second
	}

	s.stats.Start(logEach, cursor)

	s.logger.Info("starting clickhouse sink",
		zap.Duration("stats_refresh_each", logEach),
		zap.Stringer("restarting_at", cursor.Block()),
		zap.String("database", s.loader.GetDatabase()),
	)
	s.Sinker.Run(ctx, cursor, s)
}

func (s *ClickhouseSinker) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	output := data.Output

	if output.Name != s.OutputModuleName() {
		return fmt.Errorf("received data from wrong output module, expected to received from %q but got module's output for %q", s.OutputModuleName(), output.Name)
	}

	dbChanges := &pbddatabase.DatabaseChanges{}
	mapOutput := output.GetMapOutput()
	if !mapOutput.MessageIs(dbChanges) && mapOutput.TypeUrl != "type.googleapis.com/sf.substreams.database.v1.DatabaseChanges" {
		return fmt.Errorf("mismatched message type: trying to unmarshal unknown type %q", mapOutput.MessageName())
	}

	// We do not use UnmarshalTo here because we need to parse an older proto type and
	// UnmarshalTo enforces the type check. So we check manually the `TypeUrl` above and we use
	// `Unmarshal` instead which only deals with the bytes value.
	if err := proto.Unmarshal(mapOutput.Value, dbChanges); err != nil {
		return fmt.Errorf("unmarshal database changes: %w", err)
	}

	if err := s.applyDatabaseChanges(dbChanges); err != nil {
		return fmt.Errorf("apply database changes: %w", err)
	}

	if cursor.Block().Num()%s.batchBlockModulo(data, isLive) == 0 {
		flushStart := time.Now()
		if err := s.loader.Flush(ctx, s.OutputModuleHash(), cursor); err != nil {
			return fmt.Errorf("failed to flush at block %s: %w", cursor.Block(), err)
		}

		flushDuration := time.Since(flushStart)

		FlushCount.Inc()
		FlushedEntriesCount.SetUint64(s.loader.EntriesCount())
		FlushDuration.AddInt64(flushDuration.Nanoseconds())
		s.stats.RecordBlock(cursor.Block())
	}

	return nil
}

func (s *ClickhouseSinker) applyDatabaseChanges(dbChanges *pbddatabase.DatabaseChanges) error {
	for _, change := range dbChanges.TableChanges {
		if !s.loader.HasTable(change.Table) {
			return fmt.Errorf(
				"your Substreams sent us a change for a table named %s we don't know about on %s (available tables: %s)",
				change.Table,
				s.loader.GetIdentifier(),
				s.loader.GetAvailableTablesInSchema(),
			)
		}

		primaryKey := change.Pk
		changes := map[string]string{}
		for _, field := range change.Fields {
			changes[field.Name] = field.NewValue
		}

		switch change.Operation {
		case pbddatabase.TableChange_CREATE:
			err := s.loader.Insert(change.Table, primaryKey, changes)
			if err != nil {
				return fmt.Errorf("database insert: %w", err)
			}
		case pbddatabase.TableChange_UPDATE:
			err := s.loader.Update(change.Table, primaryKey, changes)
			if err != nil {
				return fmt.Errorf("database update: %w", err)
			}
		case pbddatabase.TableChange_DELETE:
			err := s.loader.Delete(change.Table, primaryKey)
			if err != nil {
				return fmt.Errorf("database delete: %w", err)
			}
		default:
			//case database.TableChange_UNSET:
		}
	}
	return nil
}

func (s *ClickhouseSinker) HandleBlockUndoSignal(ctx context.Context, data *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {
	return fmt.Errorf("received undo signal but there is no handling of undo, this is because you used `--undo-buffer-size=0` which is invalid right now")
}

func (s *ClickhouseSinker) batchBlockModulo(blockData *pbsubstreamsrpc.BlockScopedData, isLive *bool) uint64 {
	if isLive == nil {
		panic(fmt.Errorf("liveness checker has been disabled on the Sinker instance, this is invalid in the context of 'substreams-sink-clickhouse'"))
	}

	if *isLive {
		return LIVE_BLOCK_FLUSH_EACH
	}

	if s.loader.FlushInterval() > 0 {
		return uint64(s.loader.FlushInterval())
	}

	return HISTORICAL_BLOCK_FLUSH_EACH
}
