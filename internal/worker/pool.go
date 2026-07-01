package worker

// Worker pool orchestration lives in Service.Run so polling, queueing, and
// graceful shutdown can share one cancellation path.
