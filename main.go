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
	sourceTable, destTable string, columns []pgconn.FieldDescription,
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
	columns []pgconn.FieldDescription, start, end int64, totalRows *atomic.Uint64) {

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

func quoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func attemptCopy(ctx context.Context, pool *pgxpool.Pool, sourceTable, destTable string,
	columns []pgconn.FieldDescription, start, end int64) (int64, error) {

	rconn, err := pool.Acquire(ctx)
	wconn, err := pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("connection acquisition failed: %w", err)
	}
	defer rconn.Release()
	defer wconn.Release()

	tx, err := rconn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.ReadCommitted,
		AccessMode: pgx.ReadOnly,
	})

	wx, err := wconn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.ReadCommitted,
		AccessMode: pgx.ReadWrite,
	})

	if err != nil {
		return 0, fmt.Errorf("transaction start failed: %w", err)
	}

	// Declare a binary cursor for the selected rows
	cursorName := fmt.Sprintf("copy_cursor_%d_%d", start, end)
	_, err = tx.Exec(ctx,
		fmt.Sprintf("DECLARE %s BINARY CURSOR FOR SELECT * FROM %s WHERE id BETWEEN $1 AND $2",
			cursorName, sourceTable), start, end)
	if err != nil {
		return 0, fmt.Errorf("cursor creation failed: %w", err)
	}

	// Fetch all rows from the cursor
	rows, err := tx.Query(ctx, fmt.Sprintf("FETCH ALL FROM %s", cursorName))
	if err != nil {
		return 0, fmt.Errorf("cursor fetch failed: %w", err)
	}
	defer rows.Close()

	// Extract column names from the field descriptions
	columnNames := make([]string, len(columns))
	for i, fd := range columns {
		columnNames[i] = string(fd.Name)
	}
	cbuf := &bytes.Buffer{}
	for i, cn := range columnNames {
		if i != 0 {
			cbuf.WriteString(", ")
		}
		cbuf.WriteString(quoteIdentifier(cn))
	}
	quotedColumnNames := cbuf.String()

	wSql := fmt.Sprintf("copy %s ( %s ) from stdin binary;", destTable, quotedColumnNames)
	copySource := NewRowsCopySource(rows)
	commandTag, err := wx.Conn().PgConn().CopyFrom(ctx, copySource, wSql)
	copyCount := commandTag.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("copy failed: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit failed: %w", err)
	}
	if err := wx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit failed: %w", err)
	}

	return copyCount, nil
}

func shouldDropConnection(err error) bool {
	return errors.Is(err, pg.ErrConnBusy) ||
		errors.Is(err, pg.ErrClosedPool) ||
		strings.Contains(err.Error(), "connection reset")
}

// Modified to get OIDs and type information
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

	buf            bytes.Buffer // internal buffer to accumulate data for Read()
	headerWritten  bool         // true after the header has been written
	trailerWritten bool         // true after the trailer has been written
	io.Reader
}

func NewRowsCopySource(rows pgx.Rows) *RowsCopySource {
	return &RowsCopySource{rows: rows}
}

func (r *RowsCopySource) RawValues() [][]byte {
	return r.rows.RawValues()
}

func (r *RowsCopySource) Read(p []byte) (int, error) {
	// If an error has occurred and there is no buffered data, report it.
	if r.err != nil && r.buf.Len() == 0 {
		return 0, r.err
	}

	// Continue filling our internal buffer until we have at least some data
	// or until we have finished writing all parts (header, rows, trailer).
	for r.buf.Len() < len(p) && !r.trailerWritten {
		// Write the binary COPY header if not done yet.
		// The header consists of:
		//   - an 11-byte signature: "PGCOPY\n\xff\r\n\x00"
		//   - 4 bytes of flags (0)
		//   - 4 bytes of header extension length (0)
		if !r.headerWritten {
			header := []byte("PGCOPY\n\xff\r\n\x00")
			var hdrBuf bytes.Buffer
			hdrBuf.Write(header)
			var flags [4]byte
			binary.BigEndian.PutUint32(flags[:], 0)
			hdrBuf.Write(flags[:])
			var extLen [4]byte
			binary.BigEndian.PutUint32(extLen[:], 0)
			hdrBuf.Write(extLen[:])
			r.buf.Write(hdrBuf.Bytes())
			r.headerWritten = true
			continue // now there is some header data in the buffer
		}

		// Try to fetch the next row.
		if r.rows.Next() {
			rawValues := r.rows.RawValues() // each element is already in binary form
			var rowBuf bytes.Buffer

			// Write the number of columns as int16 (big-endian)
			if err := binary.Write(&rowBuf, binary.BigEndian, int16(len(rawValues))); err != nil {
				r.err = err
				break
			}

			// For each column, write:
			//   - int32 length (or -1 if NULL)
			//   - the raw bytes (if not NULL)
			for _, val := range rawValues {
				if val == nil {
					if err := binary.Write(&rowBuf, binary.BigEndian, int32(-1)); err != nil {
						r.err = err
						break
					}
				} else {
					if err := binary.Write(&rowBuf, binary.BigEndian, int32(len(val))); err != nil {
						r.err = err
						break
					}
					if _, err := rowBuf.Write(val); err != nil {
						r.err = err
						break
					}
				}
			}
			// Append the binary representation of this row to our buffer.
			r.buf.Write(rowBuf.Bytes())
		} else {
			// If there’s an error from the underlying rows, capture it.
			if err := r.rows.Err(); err != nil {
				r.err = err
				break
			}
			// No more rows; write the trailer.
			// The trailer is a 16-bit integer with the value -1 (0xFFFF).
			var trailer [2]byte
			binary.BigEndian.PutUint16(trailer[:], 0xFFFF)
			r.buf.Write(trailer[:])
			r.trailerWritten = true
			break
		}
	}

	// Read from our internal buffer into p.
	n, err := r.buf.Read(p)
	// If we returned any bytes, do not report an error even if one exists.
	if n > 0 {
		return n, nil
	}
	if r.err != nil {
		return n, r.err
	}
	// If we have finished writing (trailer has been written) and our buffer is empty, signal EOF.
	if r.trailerWritten && r.buf.Len() == 0 {
		return n, io.EOF
	}
	return n, err
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
