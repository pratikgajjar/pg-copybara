package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	pg "github.com/jackc/pgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	sourceTable := flag.String("source", "", "Source table name")
	destTable := flag.String("dest", "", "Destination table name")
	startPK := flag.Int64("start", 0, "Start primary key")
	endPK := flag.Int64("end", 0, "End primary key")
	batchSize := flag.Int64("batch", 1000, "Batch size")
	concurrency := flag.Int("concurrency", 8, "Number of concurrent workers")
	connStr := flag.String("conn", "", "PostgreSQL connection string")
	flag.Parse()

	// Validate input
	if *sourceTable == "" || *destTable == "" || *startPK >= *endPK || *connStr == "" {
		flag.Usage()
		os.Exit(1)
	}

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	// Create connection pool
	pool := createPool(ctx, *connStr, *concurrency)
	defer pool.Close()

	// Get table columns
	columns, err := getTableColumns(pool, *sourceTable)
	if err != nil {
		log.Fatalf("Failed to get columns: %v", err)
	}
	log.Println("columns=", columns)

	// Create batch generator
	batches := generateBatches(ctx, *startPK, *endPK, *batchSize)

	// Start workers
	var wg sync.WaitGroup
	var totalRows atomic.Uint64
	startTime := time.Now()

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go worker(ctx, &wg, pool, *sourceTable, *destTable, columns, batches, &totalRows)
	}

	// Start progress reporter
	go reportProgress(ctx, &totalRows, startTime)

	wg.Wait()

	log.Printf("Completed copying %d rows in %s", totalRows.Load(), time.Since(startTime))
}

func createPool(ctx context.Context, connStr string, maxConns int) *pgxpool.Pool {
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		log.Fatal(err)
	}

	// Increase pool size slightly above concurrency to handle retries
	config.MaxConns = int32(maxConns) * 2
	config.MinConns = int32(maxConns)
	config.HealthCheckPeriod = 30 * time.Second // More frequent checks
	config.MaxConnLifetime = 60 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute
	config.ConnConfig.ConnectTimeout = 30 * time.Second

	// Add connection check hook
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SELECT 1")
		return err
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		log.Fatal(err)
	}

	return pool
}

func worker(ctx context.Context, wg *sync.WaitGroup, pool *pgxpool.Pool,
	sourceTable, destTable string, columns []string,
	batches <-chan [2]int64, totalRows *atomic.Uint64) {

	defer wg.Done()

	for {
		select {
		case batch, ok := <-batches:
			if !ok {
				return
			}
			copyBatch(ctx, pool, sourceTable, destTable, columns, batch[0], batch[1], totalRows)
		case <-ctx.Done():
			return
		}
	}
}

func copyBatch(ctx context.Context, pool *pgxpool.Pool, sourceTable, destTable string,
	columns []string, start, end int64, totalRows *atomic.Uint64) {

	const maxRetries = 1
	var count int64
	var err error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Use fresh context for each attempt to prevent cancellation contamination
		attemptCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		count, err = attemptCopy(attemptCtx, pool, sourceTable, destTable, columns, start, end)
		cancel()

		if err == nil {
			totalRows.Add(uint64(count))
			return
		}

		if shouldDropConnection(err) {
			// Force pool refresh by closing bad connections
			pool.Reset()
		}

		sleepDuration := time.Duration(math.Pow(2, float64(attempt))) * time.Second
		log.Printf("Batch %d-%d failed (attempt %d): %v. Retrying in %v",
			start, end, attempt, err, sleepDuration)
		time.Sleep(sleepDuration)
	}

	log.Printf("Batch %d-%d failed after %d attempts", start, end, maxRetries)
}

func attemptCopy(ctx context.Context, pool *pgxpool.Pool, sourceTable, destTable string,
	columns []string, start, end int64) (int64, error) {

	rxConn, err := pool.Acquire(ctx)
	txConn, err := pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("connection acquisition failed: %w", err)
	}
	defer rxConn.Release()
	defer txConn.Release()

	// Validate connection before use
	if err := rxConn.Ping(ctx); err != nil {
		return 0, fmt.Errorf("connection ping failed: %w", err)
	}

	rTx, err := rxConn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.ReadCommitted,
		AccessMode: pgx.ReadOnly,
	})

	if err != nil {
		return 0, fmt.Errorf("transaction start failed: %w", err)
	}

	// Use cursor with hold for better large batch handling
	cursorId := fmt.Sprintf("%s_cursor_%d_%d", sourceTable, start, end)
	cursorStmt := fmt.Sprintf("DECLARE %s CURSOR FOR SELECT * FROM %s WHERE id BETWEEN %d and %d", cursorId, sourceTable, start, end)
	_, err = rTx.Exec(ctx, cursorStmt)
	if err != nil {
		return 0, fmt.Errorf("cursor creation failed: %w", err)
	}

	copyStmt := fmt.Sprintf("FETCH ALL FROM %s", cursorId)
	rows, err := rTx.Query(ctx, copyStmt)
	if err != nil {
		return 0, fmt.Errorf("cursor fetch failed: %w", err)
	}
	defer rows.Close()

	// Validate connection before use
	if err := txConn.Ping(ctx); err != nil {
		return 0, fmt.Errorf("connection ping failed: %w", err)
	}

	tTx, err := txConn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.ReadCommitted,
		AccessMode: pgx.ReadWrite,
	})

	defer func() {
		if err := tTx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			log.Printf("Rollback error: %v", err)
		}
	}()

	copySource := NewRowsCopySource(rows)
	count, err := tTx.CopyFrom(ctx, pgx.Identifier{destTable}, columns, copySource)
	if err != nil {
		return 0, fmt.Errorf("copy failed: %w", err)
	}

	if err := tTx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit failed: %w", err)
	}

	return count, nil
}

func shouldDropConnection(err error) bool {
	return errors.Is(err, pg.ErrConnBusy) ||
		errors.Is(err, pg.ErrClosedPool) ||
		strings.Contains(err.Error(), "connection reset")
}

func getTableColumns(pool *pgxpool.Pool, tableName string) ([]string, error) {
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	rows, err := conn.Query(context.Background(), fmt.Sprintf("SELECT * FROM %s LIMIT 0", tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for _, fd := range rows.FieldDescriptions() {
		columns = append(columns, string(fd.Name))
	}

	return columns, nil
}

func generateBatches(ctx context.Context, start, end, batchSize int64) <-chan [2]int64 {
	batches := make(chan [2]int64, 10)

	go func() {
		defer close(batches)
		for current := start; current <= end; current += batchSize {
			batchEnd := current + batchSize - 1
			if batchEnd > end {
				batchEnd = end
			}

			select {
			case batches <- [2]int64{current, batchEnd}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return batches
}

func reportProgress(ctx context.Context, totalRows *atomic.Uint64, startTime time.Time) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rows := totalRows.Load()
			duration := time.Since(startTime).Seconds()
			rate := float64(rows) / duration
			log.Printf("Progress: %d rows (%.2f rows/sec)", rows, rate)
		case <-ctx.Done():
			return
		}
	}
}

// RowsCopySource implements pgx.CopyFromSource using existing query rows
type RowsCopySource struct {
	rows   pgx.Rows
	ctx    context.Context
	err    error
	closed bool
}

func NewRowsCopySource(rows pgx.Rows) *RowsCopySource {
	return &RowsCopySource{rows: rows}
}

func (rcs *RowsCopySource) Next() bool {
	if rcs.closed || rcs.err != nil {
		return false
	}
	return rcs.rows.Next()
}

func (rcs *RowsCopySource) Values() ([]interface{}, error) {
	return rcs.rows.Values()
}

func (rcs *RowsCopySource) Err() error {
	if rcs.err != nil {
		return rcs.err
	}
	return rcs.rows.Err()
}

func (rcs *RowsCopySource) Close() {
	if !rcs.closed {
		rcs.rows.Close()
		rcs.closed = true
	}
}
