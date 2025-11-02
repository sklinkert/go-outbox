// Package postgres provides a PostgreSQL implementation of the outbox Store interface.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
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
}

// NewStore creates a new PostgreSQL store instance.
// The tableName parameter allows customization of the outbox table name.
// Prepared statements are initialized for optimal performance.
func NewStore(db *sql.DB, tableName string) (*Store, error) {
	if tableName == "" {
		tableName = "outbox_messages"
	}

	s := &Store{
		db:        db,
		tableName: tableName,
	}

	// Prepare statements for reuse
	var err error
	s.stmtFetchPending, err = db.Prepare(buildFetchPendingQuery(tableName))
	if err != nil {
		return nil, err
	}

	s.stmtMarkFailed, err = db.Prepare(buildMarkFailedQuery(tableName))
	if err != nil {
		s.stmtFetchPending.Close()
		return nil, err
	}

	return s, nil
}

// Close closes all prepared statements and releases resources.
func (s *Store) Close() error {
	if s.stmtFetchPending != nil {
		s.stmtFetchPending.Close()
	}
	if s.stmtMarkFailed != nil {
		s.stmtMarkFailed.Close()
	}
	return nil
}

// FetchPending retrieves unprocessed messages using advisory locks to prevent
// concurrent processors from fetching the same messages.
func (s *Store) FetchPending(ctx context.Context, batchSize int) ([]*outbox.Message, error) {
	rows, err := s.stmtFetchPending.QueryContext(ctx, batchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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
	defer tx.Rollback()

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
		defer tx.Rollback()
	}

	query := buildInsertQuery(s.tableName)
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	defer stmt.Close()

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

	err := scanner.Scan(
		&msg.Id,
		&msg.Topic,
		&msg.Payload,
		&headersJSON,
		&msg.IdempotencyKey,
		&msg.CreatedAt,
		&scheduledAt,
		&msg.Attempts,
		&msg.LastError,
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
