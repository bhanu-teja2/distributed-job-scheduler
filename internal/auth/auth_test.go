package auth

import (
	"github.com/google/uuid"
	"testing"
)

func TestHashKeyIsDeterministicAndDoesNotExposeSecret(t *testing.T) {
	first := HashKey("djs_secret")
	second := HashKey("djs_secret")
	if first != second {
		t.Fatal("expected deterministic hash")
	}
	if first == "djs_secret" {
		t.Fatal("hash exposed original key")
	}
}
func TestRoleHierarchy(t *testing.T) {
	admin := Principal{Role: RoleAdmin}
	operator := Principal{Role: RoleOperator}
	viewer := Principal{Role: RoleViewer}
	if !admin.Allows(RoleOperator) || !operator.Allows(RoleViewer) || viewer.Allows(RoleOperator) {
		t.Fatal("unexpected role hierarchy")
	}
}
func TestPrincipalRoundTrip(t *testing.T) {
	principal := Principal{ClientID: uuid.New(), TenantID: uuid.New(), Role: RoleAdmin}
	got, ok := FromContext(WithPrincipal(t.Context(), principal))
	if !ok || got.ClientID != principal.ClientID {
		t.Fatal("principal was not preserved")
	}
}
