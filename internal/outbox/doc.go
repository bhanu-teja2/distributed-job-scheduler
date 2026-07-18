// Package outbox relays lifecycle events committed with job state changes from
// PostgreSQL to Kafka with leasing, retries, and at-least-once delivery.
package outbox
