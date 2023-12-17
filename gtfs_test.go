package gtfs_test

// Most tests in this package run against the in-memory and storage
// backends by default. If PostgresConnStr is set, they'll also run
// against postgres.

const (
	PostgresConnStr = "" // "postgres://postgres:mysecretpassword@localhost:5432/gtfs?sslmode=disable"
)
