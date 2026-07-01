package types

type JobType string

const (
	JobTypeSendEmail           JobType = "SEND_EMAIL"
	JobTypeCallWebhook         JobType = "CALL_WEBHOOK"
	JobTypeGenerateReport      JobType = "GENERATE_REPORT"
	JobTypeProcessPaymentRetry JobType = "PROCESS_PAYMENT_RETRY"
	JobTypeSyncCustomerData    JobType = "SYNC_CUSTOMER_DATA"
	JobTypeCleanupSessions     JobType = "CLEANUP_EXPIRED_SESSIONS"
)
