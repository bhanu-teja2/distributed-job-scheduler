# ADR 0003: Tenant-Scoped API Keys and Roles

**Status:** Accepted

The scheduler is primarily service-to-service infrastructure, so high-entropy API keys are used instead of an interactive identity provider. Only hashes are stored, roles separate observation from mutation, and tenant identity is derived from the credential rather than request data.
