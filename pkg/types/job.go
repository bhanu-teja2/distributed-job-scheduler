package types

// JobType names a class of executable work accepted by the public API.
type JobType string

const (
	// JobType constants reserve identifiers for current and planned executors.
	JobTypeSendEmail           JobType = "SEND_EMAIL"
	JobTypeCallWebhook         JobType = "CALL_WEBHOOK"
	JobTypeGenerateReport      JobType = "GENERATE_REPORT"
	JobTypeProcessPaymentRetry JobType = "PROCESS_PAYMENT_RETRY"
	JobTypeSyncCustomerData    JobType = "SYNC_CUSTOMER_DATA"
	JobTypeCleanupSessions     JobType = "CLEANUP_EXPIRED_SESSIONS"
)
