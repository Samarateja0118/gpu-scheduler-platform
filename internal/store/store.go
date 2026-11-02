// Package store persists scheduler state. Postgres is the source of truth
// across restarts; the in-memory scheduler is rebuilt from it on startup.
package store

import (
	"context"
	"errors"

	"github.com/Samarateja0118/gpu-scheduler-platform/internal/scheduler"
)

// ErrNotFound is returned when a row does not exist.
var ErrNotFound = errors.New("not found")

// Store is the persistence interface used by the control plane.
type Store interface {
	UpsertTenant(ctx context.Context, t scheduler.Tenant) error
	Tenants(ctx context.Context) ([]scheduler.Tenant, error)
	UpsertNode(ctx context.Context, n scheduler.Node) error
	Nodes(ctx context.Context) ([]scheduler.Node, error)
	// SaveEvent upserts the event's job (if any) and appends the event to
	// the audit log in one transaction.
	SaveEvent(ctx context.Context, e scheduler.Event) error
	// ActiveJobs returns Pending and Running jobs for restore.
	ActiveJobs(ctx context.Context) ([]scheduler.Job, error)
	Job(ctx context.Context, id string) (*scheduler.Job, error)
	Ping(ctx context.Context) error
	Close()
}

// Nop keeps nothing; used for local runs and tests without Postgres.
type Nop struct{}

func (Nop) UpsertTenant(context.Context, scheduler.Tenant) error { return nil }
func (Nop) Tenants(context.Context) ([]scheduler.Tenant, error)  { return nil, nil }
func (Nop) UpsertNode(context.Context, scheduler.Node) error     { return nil }
func (Nop) Nodes(context.Context) ([]scheduler.Node, error)      { return nil, nil }
func (Nop) SaveEvent(context.Context, scheduler.Event) error     { return nil }
func (Nop) ActiveJobs(context.Context) ([]scheduler.Job, error)  { return nil, nil }
func (Nop) Job(context.Context, string) (*scheduler.Job, error)  { return nil, ErrNotFound }
func (Nop) Ping(context.Context) error                           { return nil }
func (Nop) Close()                                               {}
