package postgres

import (
	"fmt"
	"strings"
)

// buildFetchPendingQuery constructs the query to fetch pending messages with advisory locks.
// This query ensures that:
// 1. Only unprocessed messages are fetched (processed_at IS NULL)
// 2. Messages respect scheduled_at (if set)
// 3. Advisory locks prevent concurrent processing
// 4. Messages are ordered by creation time for FIFO processing
func buildFetchPendingQuery(tableName string) string {
	return fmt.Sprintf(`
		SELECT
			id,
			topic,
			payload,
			headers,
			idempotency_key,
			created_at,
			scheduled_at,
			attempts,
			last_error,
			processed_at
		FROM %s
		WHERE processed_at IS NULL
		  AND (scheduled_at IS NULL OR scheduled_at <= NOW())
		  AND pg_try_advisory_xact_lock(hashtext(id))
		ORDER BY created_at ASC
		LIMIT $1
	`, tableName)
}

// buildMarkSentQuery constructs the query to mark messages as sent.
// Uses soft delete approach by setting processed_at timestamp.
func buildMarkSentQuery(tableName string, count int) string {
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
	}

	return fmt.Sprintf(`
		UPDATE %s
		SET processed_at = $1
		WHERE id IN (%s)
		  AND processed_at IS NULL
	`, tableName, strings.Join(placeholders, ", "))
}

// buildMarkFailedQuery constructs the query to mark a message as failed.
func buildMarkFailedQuery(tableName string) string {
	return fmt.Sprintf(`
		UPDATE %s
		SET attempts = $1,
		    last_error = $2
		WHERE id = $3
		  AND processed_at IS NULL
	`, tableName)
}

// buildInsertQuery constructs the query to insert a new message.
// Uses ON CONFLICT to handle potential duplicate idempotency keys.
func buildInsertQuery(tableName string) string {
	return fmt.Sprintf(`
		INSERT INTO %s (
			id,
			topic,
			payload,
			headers,
			idempotency_key,
			created_at,
			scheduled_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (idempotency_key) DO NOTHING
	`, tableName)
}
