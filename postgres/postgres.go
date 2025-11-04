// Package postgres provides a PostgreSQL implementation of the outbox Store interface.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sklinkert/go-outbox"
)

// Store implements the outbox.Store interface for PostgreSQL.
type Store struct {
	db        *sql.DB
	tableName string

	// Prepared statements for performance
	stmtFetchPending *sql.Stmt
	stmtMarkFailed   *sql.Stmt

	// Advisory lock for single-processor mode
	lockKey  int64     // PostgreSQL advisory lock key (0 = disabled)
	lockConn *sql.Conn // Dedicated connection for session-level advisory lock
	hasLock  bool      // Tracks whether this instance holds the processor lock
}

// NewStore creates a new PostgreSQL store instance.
// The tableName parameter allows customization of the outbox table name.
// The lockKey parameter enables single-processor mode using PostgreSQL advisory locks.
// Set lockKey to 0 to disable (allows multiple concurrent processors).
// Set lockKey to non-zero to enable single-processor mode (recommended for strict ordering).
// Prepared statements are initialized for optimal performance.
func NewStore(db *sql.DB, tableName string, lockKey int64) (*Store, error) {
	if tableName == "" {
		tableName = "outbox_messages"
	}

	s := &Store{
		db:        db,
		tableName: tableName,
		lockKey:   lockKey,
		hasLock:   false,
	}

	// Prepare statements for reuse
	var err error
	s.stmtFetchPending, err = db.Prepare(buildFetchPendingQuery(tableName, lockKey != 0))
	if err != nil {
		return nil, err
	}

	s.stmtMarkFailed, err = db.Prepare(buildMarkFailedQuery(tableName))
	if err != nil {
		if closeErr := s.stmtFetchPending.Close(); closeErr != nil {
			return nil, fmt.Errorf("failed to prepare statement: %w (cleanup error: %v)", err, closeErr)
		}
		return nil, err
	}

	return s, nil
}

// Close closes all prepared statements and releases resources.
// If the processor lock is held, it will be released.
func (s *Store) Close() error {
	var firstErr error

	// Release processor lock if held
	if s.hasLock && s.lockKey != 0 {
		ctx := context.Background()
		if err := s.ReleaseProcessorLock(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to release processor lock: %w", err)
		}
	}

	// Close dedicated lock connection if present
	if s.lockConn != nil {
		if err := s.lockConn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to close lock connection: %w", err)
		}
		s.lockConn = nil
	}

	if s.stmtFetchPending != nil {
		if err := s.stmtFetchPending.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to close fetch pending statement: %w", err)
		}
	}
	if s.stmtMarkFailed != nil {
		if err := s.stmtMarkFailed.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to close mark failed statement: %w", err)
		}
	}
	return firstErr
}

// FetchPending retrieves unprocessed messages using advisory locks to prevent
// concurrent processors from fetching the same messages.
func (s *Store) FetchPending(ctx context.Context, batchSize int) ([]*outbox.Message, error) {
	rows, err := s.stmtFetchPending.QueryContext(ctx, batchSize)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close() //nolint:errcheck // Best effort close
	}()

	var messages []*outbox.Message
	for rows.Next() {
		msg, err := s.scanMessage(rows)
		if err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return messages, nil
}

// MarkSent marks messages as successfully published.
// This operation is idempotent and uses soft delete approach.
func (s *Store) MarkSent(ctx context.Context, messageIds []string) error {
	if len(messageIds) == 0 {
		return nil
	}

	query := buildMarkSentQuery(s.tableName, len(messageIds))

	args := make([]interface{}, len(messageIds)+1)
	args[0] = time.Now()
	for i, id := range messageIds {
		args[i+1] = id
	}

	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

// MarkFailed records failed publishing attempts with error details.
func (s *Store) MarkFailed(ctx context.Context, failures []outbox.MessageFailure) error {
	if len(failures) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback() //nolint:errcheck // Best effort rollback, ignore ErrTxDone if committed
	}()

	// Use transaction-scoped statement from prepared statement
	stmt := tx.StmtContext(ctx, s.stmtMarkFailed)

	for _, failure := range failures {
		_, err := stmt.ExecContext(ctx, failure.Attempts, failure.Error, failure.MessageId)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// Insert adds new messages to the outbox within the caller's transaction.
// This allows business logic to insert outbox messages atomically with domain changes.
func (s *Store) Insert(ctx context.Context, messages []*outbox.Message) error {
	if len(messages) == 0 {
		return nil
	}

	// Check if we're in a transaction
	tx, err := extractTx(ctx)
	if err != nil {
		// No transaction found, create one
		tx, err = s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() {
			_ = tx.Rollback() //nolint:errcheck // Best effort rollback, ignore ErrTxDone if committed
		}()
	}

	query := buildInsertQuery(s.tableName)
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	defer func() {
		_ = stmt.Close() //nolint:errcheck // Best effort close
	}()

	for _, msg := range messages {
		headersJSON, err := json.Marshal(msg.Headers)
		if err != nil {
			return err
		}

		_, err = stmt.ExecContext(
			ctx,
			msg.Id,
			msg.Topic,
			msg.Payload,
			headersJSON,
			msg.IdempotencyKey,
			msg.CreatedAt,
			msg.ScheduledAt,
		)
		if err != nil {
			return err
		}
	}

	// Only commit if we created the transaction
	if _, hasParentTx := ctx.Value(txKey{}).(*sql.Tx); !hasParentTx {
		return tx.Commit()
	}

	return nil
}

// scanMessage scans a database row into an outbox.Message.
func (s *Store) scanMessage(scanner interface {
	Scan(dest ...interface{}) error
}) (*outbox.Message, error) {
	var msg outbox.Message
	var headersJSON []byte
	var processedAt sql.NullTime
	var scheduledAt sql.NullTime
	var lastError sql.NullString

	err := scanner.Scan(
		&msg.Id,
		&msg.Topic,
		&msg.Payload,
		&headersJSON,
		&msg.IdempotencyKey,
		&msg.CreatedAt,
		&scheduledAt,
		&msg.Attempts,
		&lastError,
		&processedAt,
	)
	if err != nil {
		return nil, err
	}

	if len(headersJSON) > 0 {
		if err := json.Unmarshal(headersJSON, &msg.Headers); err != nil {
			return nil, err
		}
	}

	if processedAt.Valid {
		msg.ProcessedAt = &processedAt.Time
	}

	if scheduledAt.Valid {
		msg.ScheduledAt = &scheduledAt.Time
	}

	if lastError.Valid {
		msg.LastError = lastError.String
	}

	return &msg, nil
}

// txKey is used as a context key for storing transactions.
type txKey struct{}

// extractTx retrieves a transaction from the context if present.
func extractTx(ctx context.Context) (*sql.Tx, error) {
	if tx, ok := ctx.Value(txKey{}).(*sql.Tx); ok {
		return tx, nil
	}
	return nil, sql.ErrTxDone
}

// WithTx returns a new context with the transaction embedded.
// This allows Insert to participate in the caller's transaction.
func WithTx(ctx context.Context, tx *sql.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

// AcquireProcessorLock acquires a session-level advisory lock to ensure only one
// processor instance runs at a time. This method blocks until the lock is available.
// The lock is automatically released when the database connection is closed or when
// ReleaseProcessorLock is called.
//
// This is recommended for production use to guarantee strict FIFO message ordering
// and prevent race conditions between multiple processor instances.
//
// Returns an error if lockKey is 0 (lock disabled) or if lock acquisition fails.
func (s *Store) AcquireProcessorLock(ctx context.Context) error {
	if s.lockKey == 0 {
		return fmt.Errorf("processor lock is disabled (lockKey = 0)")
	}

	if s.hasLock {
		return nil // Already have the lock
	}

	// Get a dedicated connection for session-level lock
	if s.lockConn == nil {
		conn, err := s.db.Conn(ctx)
		if err != nil {
			return fmt.Errorf("failed to get dedicated connection: %w", err)
		}
		s.lockConn = conn
	}

	// pg_advisory_lock is session-scoped and blocks until available
	// This ensures automatic failover when a processor instance dies
	// Note: pg_advisory_lock returns void, so we don't scan a result
	_, err := s.lockConn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", s.lockKey)
	if err != nil {
		return fmt.Errorf("failed to acquire processor lock: %w", err)
	}

	s.hasLock = true
	return nil
}

// TryAcquireProcessorLock attempts to acquire the processor lock without blocking.
// Returns true if the lock was acquired, false if another instance holds it.
// This is useful for fail-fast behavior or health checks.
//
// Returns an error if lockKey is 0 (lock disabled) or if the lock check fails.
func (s *Store) TryAcquireProcessorLock(ctx context.Context) (bool, error) {
	if s.lockKey == 0 {
		return false, fmt.Errorf("processor lock is disabled (lockKey = 0)")
	}

	if s.hasLock {
		return true, nil // Already have the lock
	}

	// Get a dedicated connection for session-level lock
	if s.lockConn == nil {
		conn, err := s.db.Conn(ctx)
		if err != nil {
			return false, fmt.Errorf("failed to get dedicated connection: %w", err)
		}
		s.lockConn = conn
	}

	// pg_try_advisory_lock is non-blocking
	var lockAcquired bool
	err := s.lockConn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", s.lockKey).Scan(&lockAcquired)
	if err != nil {
		return false, fmt.Errorf("failed to try acquire processor lock: %w", err)
	}

	s.hasLock = lockAcquired
	return lockAcquired, nil
}

// ReleaseProcessorLock releases the session-level advisory lock, allowing another
// processor instance to acquire it. This is called automatically by Close().
//
// Returns an error if the lock was not held by this instance or if release fails.
func (s *Store) ReleaseProcessorLock(ctx context.Context) error {
	if s.lockKey == 0 {
		return fmt.Errorf("processor lock is disabled (lockKey = 0)")
	}

	if !s.hasLock {
		return nil // Don't have the lock, nothing to release
	}

	if s.lockConn == nil {
		return fmt.Errorf("lock connection not available")
	}

	// pg_advisory_unlock releases the session-level lock
	var lockReleased bool
	err := s.lockConn.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", s.lockKey).Scan(&lockReleased)
	if err != nil {
		return fmt.Errorf("failed to release processor lock: %w", err)
	}

	if !lockReleased {
		return fmt.Errorf("processor lock was not held by this session")
	}

	s.hasLock = false
	return nil
}

// HasProcessorLock returns true if this store instance currently holds the processor lock.
// This can be used for health checks and monitoring.
func (s *Store) HasProcessorLock() bool {
	return s.hasLock && s.lockKey != 0
}

// IsProcessorLockEnabled returns true if the processor lock is enabled (lockKey != 0).
func (s *Store) IsProcessorLockEnabled() bool {
	return s.lockKey != 0
}
