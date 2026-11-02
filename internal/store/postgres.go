package store

import (
	"context"
	_ "embed"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Samarateja0118/gpu-scheduler-platform/internal/scheduler"
)

//go:embed schema.sql
var schema string

// Postgres implements Store with pgx.
type Postgres struct {
	pool *pgxpool.Pool
}

// OpenPostgres connects and applies the schema (idempotent).
func OpenPostgres(ctx context.Context, url string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	// Retry: in Kubernetes the database may still be starting.
	var lastErr error
	for i := 0; i < 30; i++ {
		if lastErr = pool.Ping(ctx); lastErr == nil {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	if lastErr != nil {
		pool.Close()
		return nil, lastErr
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, err
	}
	return &Postgres{pool: pool}, nil
}

func (p *Postgres) Close()                         { p.pool.Close() }
func (p *Postgres) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }

func (p *Postgres) UpsertTenant(ctx context.Context, t scheduler.Tenant) error {
	_, err := p.pool.Exec(ctx, `INSERT INTO tenants (name, guaranteed, max_gpus, pool) VALUES ($1,$2,$3,$4)
		ON CONFLICT (name) DO UPDATE SET guaranteed=EXCLUDED.guaranteed, max_gpus=EXCLUDED.max_gpus, pool=EXCLUDED.pool`,
		t.Name, t.Guaranteed, t.Max, t.Pool)
	return err
}

func (p *Postgres) Tenants(ctx context.Context) ([]scheduler.Tenant, error) {
	rows, err := p.pool.Query(ctx, `SELECT name, guaranteed, max_gpus, pool FROM tenants ORDER BY name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (scheduler.Tenant, error) {
		var t scheduler.Tenant
		err := r.Scan(&t.Name, &t.Guaranteed, &t.Max, &t.Pool)
		return t, err
	})
}

func (p *Postgres) UpsertNode(ctx context.Context, n scheduler.Node) error {
	_, err := p.pool.Exec(ctx, `INSERT INTO nodes (name, gpu_type, gpus, pool, cordoned) VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (name) DO UPDATE SET gpu_type=EXCLUDED.gpu_type, gpus=EXCLUDED.gpus, pool=EXCLUDED.pool, cordoned=EXCLUDED.cordoned, updated_at=now()`,
		n.Name, n.GPUType, n.GPUs, n.Pool, n.Cordoned)
	return err
}

func (p *Postgres) Nodes(ctx context.Context) ([]scheduler.Node, error) {
	rows, err := p.pool.Query(ctx, `SELECT name, gpu_type, gpus, pool, cordoned FROM nodes ORDER BY name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (scheduler.Node, error) {
		var n scheduler.Node
		err := r.Scan(&n.Name, &n.GPUType, &n.GPUs, &n.Pool, &n.Cordoned)
		return n, err
	})
}

const jobCols = `id, tenant, priority, gpus, gpu_type, runtime_seconds, preemptible, state, node, gpu_indices,
	borrowed, preemptions, requeues, message, submitted_at, queued_at, started_at, finished_at, wait_seconds`

func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func (p *Postgres) SaveEvent(ctx context.Context, e scheduler.Event) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var jobID *string
	if j := e.Job; j != nil {
		jobID = &j.ID
		idx := make([]int32, len(j.GPUIndices))
		for i, v := range j.GPUIndices {
			idx[i] = int32(v)
		}
		_, err = tx.Exec(ctx, `INSERT INTO jobs (`+jobCols+`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
			ON CONFLICT (id) DO UPDATE SET state=EXCLUDED.state, node=EXCLUDED.node, gpu_indices=EXCLUDED.gpu_indices,
			borrowed=EXCLUDED.borrowed, preemptions=EXCLUDED.preemptions, requeues=EXCLUDED.requeues, message=EXCLUDED.message,
			queued_at=EXCLUDED.queued_at, started_at=EXCLUDED.started_at, finished_at=EXCLUDED.finished_at,
			wait_seconds=EXCLUDED.wait_seconds, updated_at=now()`,
			j.ID, j.Tenant, j.Priority, j.GPUs, j.GPUType, j.Runtime.Seconds(), j.Preemptible, string(j.State), j.Node, idx,
			j.Borrowed, j.Preemptions, j.Requeues, j.Message, j.SubmittedAt, j.QueuedAt, nullTime(j.StartedAt), nullTime(j.FinishedAt), j.Wait.Seconds())
		if err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO events (at, type, job_id, node, by_job, lost_seconds) VALUES ($1,$2,$3,$4,$5,$6)`,
		e.Time, string(e.Type), jobID, e.Node, e.By, e.Lost.Seconds())
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func scanJob(r pgx.CollectableRow) (scheduler.Job, error) {
	var j scheduler.Job
	var rt, wait float64
	var state string
	var idx []int32
	var started, finished *time.Time
	err := r.Scan(&j.ID, &j.Tenant, &j.Priority, &j.GPUs, &j.GPUType, &rt, &j.Preemptible, &state, &j.Node, &idx,
		&j.Borrowed, &j.Preemptions, &j.Requeues, &j.Message, &j.SubmittedAt, &j.QueuedAt, &started, &finished, &wait)
	if err != nil {
		return j, err
	}
	j.Runtime = time.Duration(rt * float64(time.Second))
	j.Wait = time.Duration(wait * float64(time.Second))
	j.State = scheduler.JobState(state)
	for _, v := range idx {
		j.GPUIndices = append(j.GPUIndices, int(v))
	}
	if started != nil {
		j.StartedAt = *started
	}
	if finished != nil {
		j.FinishedAt = *finished
	}
	return j, nil
}

func (p *Postgres) ActiveJobs(ctx context.Context) ([]scheduler.Job, error) {
	rows, err := p.pool.Query(ctx, `SELECT `+jobCols+` FROM jobs WHERE state IN ('Pending','Running') ORDER BY submitted_at`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanJob)
}

func (p *Postgres) Job(ctx context.Context, id string) (*scheduler.Job, error) {
	rows, err := p.pool.Query(ctx, `SELECT `+jobCols+` FROM jobs WHERE id=$1`, id)
	if err != nil {
		return nil, err
	}
	j, err := pgx.CollectExactlyOneRow(rows, scanJob)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &j, err
}

// Exec runs a statement; used by tests and admin tooling.
func (p *Postgres) Exec(ctx context.Context, sql string) (int64, error) {
	tag, err := p.pool.Exec(ctx, sql)
	return tag.RowsAffected(), err
}
