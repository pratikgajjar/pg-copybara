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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	pgtype "github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	copyHeader  = []byte("PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00\x00\x00\x00\x00")
	copyTrailer = []byte{0xff, 0xff}
)

type jobArgs struct {
	srcPool     *pgxpool.Pool
	destPool    *pgxpool.Pool
	snapshotID  string
	sourceTable string
	destTable   string
	copySQL     string
	destColumns []pgconn.FieldDescription
}

func main() {
	sourceTable := flag.String("source", "", "Source table name")
	destTable := flag.String("dest", "", "Destination table name (defaults to source)")
	blockBatchSize := flag.Int64("block-batch-size", 1000, "Number of blocks per batch")
	concurrency := flag.Int("concurrency", 8, "Number of concurrent workers")
	sourceConnStr := flag.String("sourceConn", "", "Source PostgreSQL connection string")
	destConnStr := flag.String("destConn", "", "Destination PostgreSQL connection string (defaults to source)")
	flag.Parse()

	if *sourceTable == "" || *sourceConnStr == "" {
		flag.Usage()
		os.Exit(1)
	}

	if *destTable == "" {
		*destTable = *sourceTable
	}

	if *destConnStr == "" {
		*destConnStr = *sourceConnStr
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	srcPool := createPool(ctx, *sourceConnStr, *concurrency)
	defer srcPool.Close()
	destPool := createPool(ctx, *destConnStr, *concurrency)
	defer destPool.Close()

	// Start a transaction to get a consistent snapshot
	// Create pgx connection directly here
	sconf, err := pgx.ParseConfig(*sourceConnStr)
	if err != nil {
		log.Fatalf("Failed to parse source connection string: %v", err)
	}
	scon, err := pgx.ConnectConfig(ctx, sconf)
	if err != nil {
		log.Fatalf("Failed to connect to source database: %v", err)
	}
	// Export the snapshot
	tx, err := scon.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		log.Fatalf("Failed to start transaction: %v", err)
	}
	var snapshotID string
	err = scon.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshotID)
	if err != nil {
		log.Fatalf("Failed to export snapshot: %v", err)
	}
	log.Println("Snapshot ID:", snapshotID)

	// Now get the CTID range within this transaction
	_, mx, err := getCTIDBlockRangeInTx(tx, *sourceTable)
	if err != nil {
		log.Fatalf("Failed to get CTID range: %v", err)
	}

	// Keep transaction open until all workers complete
	defer tx.Rollback(ctx)
	defer scon.Close(ctx)

	destColumns, err := getTableColumns(destPool, *destTable)
	if err != nil {
		log.Fatalf("Failed to get destination columns: %v", err)
	}

	quotedColumns := generateQuotedColumns(destColumns)
	copySQL := fmt.Sprintf("COPY %s (%s) FROM STDIN BINARY", *destTable, quotedColumns)

	batches := generateBlockBatches(ctx, mx+1, *blockBatchSize)

	args := &jobArgs{
		srcPool:     srcPool,
		destPool:    destPool,
		sourceTable: *sourceTable,
		destTable:   *destTable,
		copySQL:     copySQL,
		destColumns: destColumns,
		snapshotID:  snapshotID,
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
	if err := verifyRowCounts(ctx, args); err != nil {
		log.Printf("WARNING: %v", err)
	} else {
		log.Println("COUNT Equal = source = dest")
	}
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

func generateBlockBatches(ctx context.Context, totalBlocks, blockBatchSize int64) <-chan [2]int64 {
	batches := make(chan [2]int64, 100)
	go func() {
		defer close(batches)
		log.Printf("Generating batches for %d total blocks with batch size %d", totalBlocks, blockBatchSize)
		for start := int64(0); start < totalBlocks; start += blockBatchSize {
			end := start + blockBatchSize - 1
			if end >= totalBlocks {
				end = totalBlocks
			}
			select {
			case batches <- [2]int64{start, end}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return batches
}

func copyBatch(ctx context.Context, startBlock, endBlock int64, totalRows *atomic.Uint64, args *jobArgs) {
	const maxRetries = 1
	var count int64
	var err error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		attemptCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		count, err = attemptCopy(attemptCtx, startBlock, endBlock, args)
		cancel()

		if err == nil {
			totalRows.Add(uint64(count))
			return
		} else {
			log.Println("block-range-failed", startBlock, endBlock)
		}

		if shouldDropConnection(err) {
			args.srcPool.Reset()
			args.destPool.Reset()
		}

		if maxRetries > 1 {
			time.Sleep(time.Duration(1<<attempt) * time.Second)
		}
	}
}

func attemptCopy(ctx context.Context, startBlock, endBlock int64, args *jobArgs) (int64, error) {
	startCTID := fmt.Sprintf("(%d,0)", startBlock)
	endCTID := fmt.Sprintf("(%d,0)", endBlock+1)
	log.Println("attempting-copy", startBlock, endBlock)

	rconn, err := args.srcPool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire read connection failed: %w", err)
	}
	defer rconn.Release()

	wconn, err := args.destPool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire write connection failed: %w", err)
	}
	defer wconn.Release()

	rtx, err := rconn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return 0, fmt.Errorf("begin read tx failed: %w", err)
	}
	defer rtx.Rollback(ctx)

	if args.snapshotID != "" {
		if _, err := rtx.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", args.snapshotID)); err != nil {
			return 0, fmt.Errorf("set transaction snapshot failed: %w", err)
		}
	}

	cursorName := fmt.Sprintf("copy_cursor_%d_%d", startBlock, endBlock)
	_, err = rtx.Exec(ctx, fmt.Sprintf(`
		DECLARE %s BINARY CURSOR FOR
		SELECT * FROM %s
		WHERE ctid >= $1::tid AND ctid < $2::tid`,
		cursorName, args.sourceTable), startCTID, endCTID)
	if err != nil {
		return 0, fmt.Errorf("declare cursor failed: %w", err)
	}

	rows, err := rtx.Query(ctx, fmt.Sprintf("FETCH ALL FROM %s", cursorName))
	if err != nil {
		return 0, fmt.Errorf("fetch rows failed: %w", err)
	}
	defer rows.Close()

	wtx, err := wconn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite})
	if err != nil {
		return 0, fmt.Errorf("begin write tx failed: %w", err)
	}
	defer wtx.Rollback(ctx)

	copySource := NewRowsCopySource(rows, args.destColumns)
	cmdTag, err := wtx.Conn().PgConn().CopyFrom(ctx, copySource, args.copySQL)
	if err != nil {
		wtx.Rollback(ctx)
		log.Println("type=copy res=err startBlock=", startBlock, "endBlock=", endBlock, "copy_err=", err)
		rowsAffected, er := copySource.BulkInsert(ctx, wconn, args.destTable)
		if er != nil {
			log.Println("try: insert", "rows=", rowsAffected)
			rtx.Commit(ctx)
			return rowsAffected, nil
		}
		return 0, fmt.Errorf("copy failed: %w", err)
	}
	log.Println("type=copy", "status=success", "startBlock=", startBlock, "endBlock=", endBlock)

	if err := rtx.Commit(ctx); err != nil {
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
	capturedValues [][]interface{} // Store decoded values

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
			vals, err := r.rows.Values()
			if err != nil {
				r.err = err
				return 0, err
			}
			r.capturedValues = append(r.capturedValues, vals)

			var numCols [2]byte
			binary.BigEndian.PutUint16(numCols[:], uint16(len(rawValues)))
			r.rowBuf.Write(numCols[:])

			for i, val := range rawValues {
				if val == nil {
					r.rowBuf.Write([]byte{0xff, 0xff, 0xff, 0xff})
				} else {
					// Use destination column info for type conversion.
					switch r.destColumns[i].DataTypeOID {
					case pgtype.JSONBOID:
						val = append([]byte{1}, val...) // Add JSONB versioning byte.
						break
					case pgtype.Int8OID:
						if len(val) == 4 {
							// Convert 4-byte INT to 8-byte BIGINT
							v := int32(binary.BigEndian.Uint32(val))
							var buf [8]byte
							binary.BigEndian.PutUint64(buf[:], uint64(v))
							val = buf[:]
						}
						break
					}
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

func (r *RowsCopySource) BulkInsert(ctx context.Context, conn *pgxpool.Conn, destTable string) (int64, error) {
	if len(r.capturedValues) == 0 {
		return 0, nil
	}

	quotedColumns := generateQuotedColumns(r.destColumns)

	// Build one big INSERT statement with multiple value groups
	var placeholders []string
	var args []interface{}
	for _, row := range r.capturedValues {
		ph := make([]string, len(row))
		for j := range row {
			args = append(args, row[j])
			ph[j] = fmt.Sprintf("$%d", len(args))
		}
		placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(ph, ",")))
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s ON CONFLICT DO NOTHING",
		destTable,
		quotedColumns,
		strings.Join(placeholders, ","),
	)

	cmdTag, err := conn.Exec(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("bulk insert failed: %w", err)
	}

	return cmdTag.RowsAffected(), nil
}

// getCTIDBlockRange fetches the minimum and maximum CTID block numbers using pgtype.TID.
func getCTIDBlockRangeInTx(tx pgx.Tx, tableName string) (int64, int64, error) {
	var maxBlocks int64
	var minTid, maxTid pgtype.TID
	err := tx.QueryRow(context.Background(),
		fmt.Sprintf(`SELECT MIN(ctid), MAX(ctid) FROM %s`, tableName)).Scan(&minTid, &maxTid)
	if err != nil {
		return 0, 0, err
	}
	maxBlocks = int64(maxTid.BlockNumber) + 1
	log.Printf("CTID block range: min=%d, max=%d (total blocks: %d)", minTid.BlockNumber, maxTid.BlockNumber, maxBlocks)
	return int64(minTid.BlockNumber), maxBlocks, nil
}

func verifyRowCounts(ctx context.Context, args *jobArgs) error {
	var sourceCount, destCount int64

	err := args.srcPool.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", args.sourceTable)).Scan(&sourceCount)
	if err != nil {
		return fmt.Errorf("failed to get source count: %w", err)
	}

	err = args.destPool.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", args.destTable)).Scan(&destCount)
	if err != nil {
		return fmt.Errorf("failed to get destination count: %w", err)
	}

	if sourceCount != destCount {
		return fmt.Errorf("row count mismatch: source=%d dest=%d diff=%d",
			sourceCount, destCount, sourceCount-destCount)
	}

	return nil
}
