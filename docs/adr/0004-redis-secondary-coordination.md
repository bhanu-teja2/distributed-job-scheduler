# ADR 0004: Redis Is a Secondary Coordination Layer

**Status:** Accepted

PostgreSQL row ownership prevents duplicate claims. Redis adds fast owner-checked leases and worker heartbeats, but losing Redis cannot transfer job ownership or overwrite database state. Workers stop protected execution when lease renewal fails.
