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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	copyHeader  = []byte("PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00\x00\x00\x00\x00")
	copyTrailer = []byte{0xff, 0xff}
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

	if *sourceTable == "" || *destTable == "" || *startPK >= *endPK || *connStr == "" {
		flag.Usage()
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	pool := createPool(ctx, *connStr, *concurrency)
	defer pool.Close()

	columns, err := getTableColumns(pool, *sourceTable)
	if err != nil {
		log.Fatalf("Failed to get columns: %v", err)
	}

	quotedColumns := generateQuotedColumns(columns)
	copySQL := fmt.Sprintf("COPY %s (%s) FROM STDIN BINARY", *destTable, quotedColumns)
	log.Println(copySQL)

	batches := generateBatches(ctx, *startPK, *endPK, *batchSize)

	var wg sync.WaitGroup
	var totalRows atomic.Uint64
	startTime := time.Now()

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go worker(ctx, &wg, pool, *sourceTable, copySQL, batches, &totalRows)
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

	config.MaxConns = int32(maxConns) * 2
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

func worker(ctx context.Context, wg *sync.WaitGroup, pool *pgxpool.Pool,
	sourceTable, copySQL string, batches <-chan [2]int64, totalRows *atomic.Uint64) {

	defer wg.Done()

	for {
		select {
		case batch, ok := <-batches:
			if !ok {
				return
			}
			copyBatch(ctx, pool, sourceTable, copySQL, batch[0], batch[1], totalRows)
		case <-ctx.Done():
			return
		}
	}
}

func copyBatch(ctx context.Context, pool *pgxpool.Pool, sourceTable, copySQL string,
	start, end int64, totalRows *atomic.Uint64) {

	const maxRetries = 1
	var count int64
	var err error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		attemptCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		count, err = attemptCopy(attemptCtx, pool, sourceTable, copySQL, start, end)
		cancel()

		if err == nil {
			totalRows.Add(uint64(count))
			return
		}

		if shouldDropConnection(err) {
			pool.Reset()
		}

		sleepDuration := time.Duration(math.Pow(2, float64(attempt))) * time.Second
		log.Printf("Batch %d-%d failed (attempt %d): %v. Retrying in %v",
			start, end, attempt, err, sleepDuration)
		time.Sleep(sleepDuration)
	}

	log.Printf("Batch %d-%d failed after %d attempts", start, end, maxRetries)
}

func attemptCopy(ctx context.Context, pool *pgxpool.Pool, sourceTable, copySQL string,
	start, end int64) (int64, error) {

	rconn, err := pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire read connection failed: %w", err)
	}
	defer rconn.Release()

	wconn, err := pool.Acquire(ctx)
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
	_, err = tx.Exec(ctx, fmt.Sprintf("DECLARE %s BINARY CURSOR FOR SELECT * FROM %s WHERE id BETWEEN $1 AND $2", cursorName, sourceTable), start, end)
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

	copySource := NewRowsCopySource(rows)
	cmdTag, err := wtx.Conn().PgConn().CopyFrom(ctx, copySource, copySQL)
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

type RowsCopySource struct {
	rows           pgx.Rows
	headerWritten  bool
	trailerWritten bool
	err            error
	closed         bool
	buf            bytes.Buffer
	rowBuf         bytes.Buffer
}

func NewRowsCopySource(rows pgx.Rows) *RowsCopySource {
	return &RowsCopySource{rows: rows}
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

			for _, val := range rawValues {
				if val == nil {
					r.rowBuf.Write([]byte{0xff, 0xff, 0xff, 0xff})
				} else {
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
