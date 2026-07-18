package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Role is an API client's authorization level within one tenant.
type Role string

const (
	// RoleViewer permits read-only access to jobs and operational data.
	RoleViewer Role = "viewer"
	// RoleOperator adds job creation and lifecycle operations.
	RoleOperator Role = "operator"
	// RoleAdmin adds credential management and event replay operations.
	RoleAdmin Role = "admin"
)

// DefaultTenantID identifies data backfilled before tenant support existed.
var DefaultTenantID = uuid.MustParse("00000000-0000-0000-0000-000000000001")

// Principal is the authenticated API client and tenant attached to a request.
type Principal struct {
	ClientID   uuid.UUID `json:"client_id"`
	TenantID   uuid.UUID `json:"tenant_id"`
	TenantSlug string    `json:"tenant_slug"`
	ClientName string    `json:"client_name"`
	Role       Role      `json:"role"`
}

type contextKey struct{}

// WithPrincipal returns a context carrying the authenticated principal.
func WithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, contextKey{}, principal)
}

// FromContext retrieves the authenticated principal from a context.
func FromContext(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(contextKey{}).(Principal)
	return principal, ok
}

// PrincipalOrDefault returns the request principal or the privileged system
// principal used by explicitly unauthenticated local-development paths.
func PrincipalOrDefault(ctx context.Context) Principal {
	if principal, ok := FromContext(ctx); ok {
		return principal
	}
	return Principal{TenantID: DefaultTenantID, TenantSlug: "default", ClientName: "system", Role: RoleAdmin}
}

// Allows reports whether the principal's role includes the required role.
func (p Principal) Allows(required Role) bool {
	rank := map[Role]int{RoleViewer: 1, RoleOperator: 2, RoleAdmin: 3}
	return rank[p.Role] >= rank[required]
}

// Authenticator resolves a raw API key into its tenant-scoped principal.
type Authenticator interface {
	Authenticate(ctx context.Context, apiKey string) (Principal, error)
}

// ErrUnauthorized indicates that an API key is missing, revoked, or unknown.
var ErrUnauthorized = errors.New("invalid API key")

// Store persists API clients and implements Authenticator with PostgreSQL.
type Store struct{ pool *pgxpool.Pool }

// NewStore creates a PostgreSQL-backed authentication store.
func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Authenticate validates a key hash and returns the active API client.
func (s *Store) Authenticate(ctx context.Context, apiKey string) (Principal, error) {
	hash := HashKey(strings.TrimSpace(apiKey))
	var principal Principal
	var storedHash string
	err := s.pool.QueryRow(ctx, `SELECT c.id, c.tenant_id, t.slug, c.name, c.role, c.key_hash
FROM api_clients c JOIN tenants t ON t.id=c.tenant_id
WHERE c.key_hash=$1 AND c.enabled=true AND c.revoked_at IS NULL`, hash).
		Scan(&principal.ClientID, &principal.TenantID, &principal.TenantSlug, &principal.ClientName, &principal.Role, &storedHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return Principal{}, ErrUnauthorized
	}
	if err != nil {
		return Principal{}, err
	}
	if subtle.ConstantTimeCompare([]byte(storedHash), []byte(hash)) != 1 {
		return Principal{}, ErrUnauthorized
	}
	_, _ = s.pool.Exec(ctx, `UPDATE api_clients SET last_used_at=$2 WHERE id=$1`, principal.ClientID, time.Now().UTC())
	return principal, nil
}

// CreateClient creates an API client and returns its secret exactly once. A
// supplied key is supported for deterministic local-development bootstrap.
func (s *Store) CreateClient(ctx context.Context, tenantID uuid.UUID, name string, role Role, suppliedKey string) (uuid.UUID, string, error) {
	key := strings.TrimSpace(suppliedKey)
	if key == "" {
		var err error
		key, err = GenerateKey()
		if err != nil {
			return uuid.Nil, "", err
		}
	}
	id := uuid.New()
	prefix := key
	if len(prefix) > 14 {
		prefix = prefix[:14]
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO api_clients(id, tenant_id, name, key_prefix, key_hash, role)
VALUES($1,$2,$3,$4,$5,$6)`, id, tenantID, name, prefix, HashKey(key), role)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" && suppliedKey != "" {
		var existing uuid.UUID
		lookupErr := s.pool.QueryRow(ctx, `SELECT id FROM api_clients WHERE key_hash=$1`, HashKey(key)).Scan(&existing)
		return existing, key, lookupErr
	}
	return id, key, err
}

// RevokeClient permanently disables an API client without deleting audit data.
func (s *Store) RevokeClient(ctx context.Context, id uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, `UPDATE api_clients SET enabled=false, revoked_at=now() WHERE id=$1`, id)
	if err == nil && tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return err
}

// HashKey returns the SHA-256 representation persisted instead of a raw key.
func HashKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// GenerateKey creates a high-entropy, URL-safe API key with a recognizable prefix.
func GenerateKey() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return "djs_" + base64.RawURLEncoding.EncodeToString(raw), nil
}
