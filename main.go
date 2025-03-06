package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	pg "github.com/jackc/pgx"
	pgtype "github.com/jackc/pgx/pgtype"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	copyHeader  = []byte("PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00\x00\x00\x00\x00")
	copyTrailer = []byte{0xff, 0xff}
)

// jobArgs bundles all the parameters needed for the job, including
// both connection pools, source table, COPY SQL, and destination columns.
type jobArgs struct {
	srcPool     *pgxpool.Pool
	destPool    *pgxpool.Pool
	sourceTable string
	copySQL     string
	destColumns []pgconn.FieldDescription
}

func main() {
	sourceTable := flag.String("source", "", "Source table name")
	destTable := flag.String("dest", "", "Destination table name (if empty, defaults to source table)")
	startPK := flag.Int64("start", 0, "Start primary key")
	endPK := flag.Int64("end", 0, "End primary key")
	batchSize := flag.Int64("batch", 1000, "Batch size")
	concurrency := flag.Int("concurrency", 8, "Number of concurrent workers")
	sourceConnStr := flag.String("sourceConn", "", "PostgreSQL connection string for source")
	destConnStr := flag.String("destConn", "", "PostgreSQL connection string for destination (if empty, defaults to source connection string)")
	flag.Parse()

	if *sourceTable == "" || *startPK >= *endPK || *sourceConnStr == "" {
		flag.Usage()
		os.Exit(1)
	}

	if *destTable == "" {
		*destTable = *sourceTable
		log.Println("destTable=", destTable)
	}

	if *destConnStr == "" {
		*destConnStr = *sourceConnStr
		log.Println("#WARN using destination db string same as source")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	// Create connection pools for source and destination.
	srcPool := createPool(ctx, *sourceConnStr, *concurrency)
	defer srcPool.Close()
	destPool := createPool(ctx, *destConnStr, *concurrency)
	defer destPool.Close()

	// Retrieve destination table column definitions (used for COPY and type conversion).
	destColumns, err := getTableColumns(destPool, *destTable)
	if err != nil {
		log.Fatalf("Failed to get destination columns: %v", err)
	}

	quotedColumns := generateQuotedColumns(destColumns)
	copySQL := fmt.Sprintf("COPY %s (%s) FROM STDIN BINARY", *destTable, quotedColumns)
	log.Println(copySQL)

	batches := generateBatches(ctx, *startPK, *endPK, *batchSize)

	// Bundle all configuration parameters (including pools) into jobArgs.
	args := &jobArgs{
		srcPool:     srcPool,
		destPool:    destPool,
		sourceTable: *sourceTable,
		copySQL:     copySQL,
		destColumns: destColumns,
	}

	var wg sync.WaitGroup
	var totalRows atomic.Uint64
	startTime := time.Now()

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go worker(ctx, &wg, batches, &totalRows, args)
	}

	go reportProgress(ctx, &totalRows, startTime)

	wg.Wait()

	log.Printf("Completed copying %d rows in %s", totalRows.Load(), time.Since(startTime))
}

func createPool(ctx context.Context, connStr string, maxConns int) *pgxpool.Pool {
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		log.Fatal(err)
	}

	config.MaxConns = int32(maxConns)
	config.MinConns = int32(maxConns)
	config.HealthCheckPeriod = 30 * time.Second
	config.MaxConnLifetime = 60 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute
	config.ConnConfig.ConnectTimeout = 30 * time.Second

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

func worker(ctx context.Context, wg *sync.WaitGroup, batches <-chan [2]int64, totalRows *atomic.Uint64, args *jobArgs) {
	defer wg.Done()

	for {
		select {
		case batch, ok := <-batches:
			if !ok {
				return
			}
			// Process the batch range with adaptive subdivision.
			copyBatch(ctx, batch[0], batch[1], totalRows, args)
		case <-ctx.Done():
			return
		}
	}
}

// copyBatch attempts to copy rows in the range [start, end].
// If the operation fails after maxRetries, it subdivides the range
// (reducing its size by a factor of 10) and processes each subrange recursively.
// For a single row (batch size of 1) that fails, the row is skipped.
func copyBatch(ctx context.Context, start, end int64, totalRows *atomic.Uint64, args *jobArgs) {
	const maxRetries = 1
	var count int64
	var err error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		attemptCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		count, err = attemptCopy(attemptCtx, start, end, args)
		cancel()

		if err == nil {
			totalRows.Add(uint64(count))
			return
		}

		if shouldDropConnection(err) {
			args.srcPool.Reset()
			args.destPool.Reset()
		}

		if maxRetries > 1 {
			sleepDuration := time.Duration(1<<attempt) * time.Second
			log.Printf("Batch %d-%d failed (attempt %d): %v. Retrying in %v", start, end, attempt, err, sleepDuration)
			time.Sleep(sleepDuration)
		}
	}

	// If we've reached here, the copy failed after maxRetries.
	batchSize := end - start + 1
	if batchSize == 1 {
		log.Printf("Single row %d failed, skipping: %v", start, err)
		return
	}

	newBatchSize := batchSize / 10
	if newBatchSize < 1 {
		newBatchSize = 1
	}
	log.Printf("Dividing batch %d-%d (size %d) into smaller batches of size %d due to errors: %v",
		start, end, batchSize, newBatchSize, err)

	// Recursively process sub-batches.
	for subStart := start; subStart <= end; subStart += newBatchSize {
		subEnd := subStart + newBatchSize - 1
		if subEnd > end {
			subEnd = end
		}
		copyBatch(ctx, subStart, subEnd, totalRows, args)
	}
}

func attemptCopy(ctx context.Context, start, end int64, args *jobArgs) (int64, error) {
	// Acquire a read connection from the source pool.
	rconn, err := args.srcPool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire read connection failed: %w", err)
	}
	defer rconn.Release()

	// Acquire a write connection from the destination pool.
	wconn, err := args.destPool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire write connection failed: %w", err)
	}
	defer wconn.Release()

	tx, err := rconn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadOnly})
	if err != nil {
		return 0, fmt.Errorf("begin read tx failed: %w", err)
	}
	defer tx.Rollback(ctx)

	cursorName := fmt.Sprintf("copy_cursor_%d_%d", start, end)
	_, err = tx.Exec(ctx, fmt.Sprintf("DECLARE %s BINARY CURSOR FOR SELECT * FROM %s WHERE id BETWEEN $1 AND $2",
		cursorName, args.sourceTable), start, end)
	if err != nil {
		return 0, fmt.Errorf("declare cursor failed: %w", err)
	}

	rows, err := tx.Query(ctx, fmt.Sprintf("FETCH ALL FROM %s", cursorName))
	if err != nil {
		return 0, fmt.Errorf("fetch rows failed: %w", err)
	}
	defer rows.Close()

	wtx, err := wconn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite})
	if err != nil {
		return 0, fmt.Errorf("begin write tx failed: %w", err)
	}
	defer wtx.Rollback(ctx)

	// Pass the destination columns to the copy source.
	copySource := NewRowsCopySource(rows, args.destColumns)
	cmdTag, err := wtx.Conn().PgConn().CopyFrom(ctx, copySource, args.copySQL)
	if err != nil {
		return 0, fmt.Errorf("copy failed: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit read tx failed: %w", err)
	}
	if err := wtx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit write tx failed: %w", err)
	}

	return cmdTag.RowsAffected(), nil
}

func shouldDropConnection(err error) bool {
	return errors.Is(err, pg.ErrDeadConn) ||
		strings.Contains(err.Error(), "connection reset")
}

func generateQuotedColumns(columns []pgconn.FieldDescription) string {
	var b strings.Builder
	for i, col := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(`"` + strings.ReplaceAll(string(col.Name), `"`, `""`) + `"`)
	}
	return b.String()
}

func getTableColumns(pool *pgxpool.Pool, tableName string) ([]pgconn.FieldDescription, error) {
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

	return rows.FieldDescriptions(), nil
}

func generateBatches(ctx context.Context, start, end, batchSize int64) <-chan [2]int64 {
	batches := make(chan [2]int64, 100)
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

// RowsCopySource holds destination column definitions for type conversion.
type RowsCopySource struct {
	rows           pgx.Rows
	headerWritten  bool
	trailerWritten bool
	err            error
	closed         bool
	buf            bytes.Buffer
	rowBuf         bytes.Buffer
	destColumns    []pgconn.FieldDescription
}

func NewRowsCopySource(rows pgx.Rows, destColumns []pgconn.FieldDescription) *RowsCopySource {
	return &RowsCopySource{
		rows:        rows,
		destColumns: destColumns,
	}
}

func (r *RowsCopySource) Read(p []byte) (int, error) {
	if r.err != nil && r.buf.Len() == 0 {
		return 0, r.err
	}

	for r.buf.Len() < len(p) && !r.trailerWritten {
		if !r.headerWritten {
			r.buf.Write(copyHeader)
			r.headerWritten = true
			continue
		}

		if r.rows.Next() {
			r.rowBuf.Reset()
			rawValues := r.rows.RawValues()

			var numCols [2]byte
			binary.BigEndian.PutUint16(numCols[:], uint16(len(rawValues)))
			r.rowBuf.Write(numCols[:])

			for i, val := range rawValues {
				if val == nil {
					r.rowBuf.Write([]byte{0xff, 0xff, 0xff, 0xff})
				} else {
					// Use destination column info for type conversion.
					if r.destColumns[i].DataTypeOID == pgtype.JSONBOID {
						val = append([]byte{1}, val...) // Add JSONB versioning byte.
					}
					// Additional type-specific logic can be added here.
					lenBytes := make([]byte, 4)
					binary.BigEndian.PutUint32(lenBytes, uint32(len(val)))
					r.rowBuf.Write(lenBytes)
					r.rowBuf.Write(val)
				}
			}

			r.buf.Write(r.rowBuf.Bytes())
		} else {
			if err := r.rows.Err(); err != nil {
				r.err = err
				break
			}
			r.buf.Write(copyTrailer)
			r.trailerWritten = true
			break
		}
	}

	n, err := r.buf.Read(p)
	if r.buf.Len() == 0 {
		r.buf.Reset()
	}
	if n > 0 {
		return n, nil
	}
	if r.err != nil {
		return n, r.err
	}
	if r.trailerWritten {
		return n, io.EOF
	}
	return n, err
}

func (r *RowsCopySource) Close() {
	if !r.closed {
		r.rows.Close()
		r.closed = true
	}
}
