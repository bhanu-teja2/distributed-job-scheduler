package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/bhanuteja/distributed-job-scheduler/internal/auth"
	"github.com/bhanuteja/distributed-job-scheduler/internal/config"
	"github.com/bhanuteja/distributed-job-scheduler/internal/postgres"
	"github.com/google/uuid"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cfg := config.Load()
	ctx := context.Background()
	db, err := postgres.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		fatal(err)
	}
	defer db.Close()
	store := auth.NewStore(db)
	switch os.Args[1] {
	case "create-api-key":
		flags := flag.NewFlagSet("create-api-key", flag.ExitOnError)
		tenant := flags.String("tenant", auth.DefaultTenantID.String(), "tenant UUID")
		name := flags.String("name", "local-admin", "client name")
		role := flags.String("role", string(auth.RoleAdmin), "viewer, operator, or admin")
		key := flags.String("key", "", "optional supplied development key")
		_ = flags.Parse(os.Args[2:])
		tenantID, err := uuid.Parse(*tenant)
		if err != nil {
			fatal(err)
		}
		parsedRole := auth.Role(*role)
		if parsedRole != auth.RoleViewer && parsedRole != auth.RoleOperator && parsedRole != auth.RoleAdmin {
			fatal(fmt.Errorf("invalid role %q", *role))
		}
		id, secret, err := store.CreateClient(ctx, tenantID, *name, parsedRole, *key)
		if err != nil {
			fatal(err)
		}
		fmt.Printf("client_id=%s\napi_key=%s\n", id, secret)
	case "revoke-api-key":
		flags := flag.NewFlagSet("revoke-api-key", flag.ExitOnError)
		idValue := flags.String("id", "", "client UUID")
		_ = flags.Parse(os.Args[2:])
		id, err := uuid.Parse(*idValue)
		if err != nil {
			fatal(err)
		}
		if err = store.RevokeClient(ctx, id); err != nil {
			fatal(err)
		}
		fmt.Printf("revoked=%s\n", id)
	default:
		usage()
		os.Exit(2)
	}
}
func usage()          { fmt.Fprintln(os.Stderr, "usage: scheduler-admin create-api-key|revoke-api-key [flags]") }
func fatal(err error) { fmt.Fprintln(os.Stderr, "error:", err); os.Exit(1) }
