package publication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
)

const postgresPublicationSchema = `
CREATE SCHEMA IF NOT EXISTS adl_publication;

CREATE TABLE IF NOT EXISTS adl_publication.runs (
  execution_key text PRIMARY KEY,
  case_id text NOT NULL,
  probe text NOT NULL,
  trial integer NOT NULL CHECK (trial > 0),
  state text NOT NULL CHECK (state IN ('running', 'completed')),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  completed_at timestamptz
);

CREATE TABLE IF NOT EXISTS adl_publication.jobs (
  execution_key text NOT NULL REFERENCES adl_publication.runs(execution_key),
  round_sequence integer NOT NULL CHECK (round_sequence > 0),
  work_id text NOT NULL,
  input jsonb NOT NULL,
  state text NOT NULL CHECK (state IN ('queued', 'running', 'completed', 'failed')),
  generation bigint NOT NULL DEFAULT 0 CHECK (generation >= 0),
  lease_owner text,
  enqueued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  started_at timestamptz,
  completed_at timestamptz,
  PRIMARY KEY (execution_key, round_sequence, work_id)
);

CREATE INDEX IF NOT EXISTS publication_jobs_claim
ON adl_publication.jobs(execution_key, round_sequence, state, work_id);

CREATE TABLE IF NOT EXISTS adl_publication.authority (
  execution_key text PRIMARY KEY REFERENCES adl_publication.runs(execution_key),
  owner_id text NOT NULL,
  generation bigint NOT NULL CHECK (generation > 0),
  capability_hash text NOT NULL,
  stopped boolean NOT NULL DEFAULT false,
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS adl_publication.outcomes (
  execution_key text PRIMARY KEY REFERENCES adl_publication.runs(execution_key),
  acknowledged boolean NOT NULL,
  completed_work integer NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS adl_publication.journal (
  journal_id bigserial PRIMARY KEY,
  execution_key text NOT NULL REFERENCES adl_publication.runs(execution_key),
  event_time timestamptz NOT NULL DEFAULT clock_timestamp(),
  kind text NOT NULL,
  detail jsonb NOT NULL
);
`

type PostgreSQLExecutorConfig struct {
	DSN               string
	EvidenceRoot      string
	AdapterVersion    string
	AgentBinarySHA256 string
}

type PostgreSQLTimedExecutor struct {
	config        PostgreSQLExecutorConfig
	pool          *pgxpool.Pool
	serverVersion string
	mu            sync.Mutex
	active        int
	closed        bool
}

func OpenPostgreSQLTimedExecutor(ctx context.Context, config PostgreSQLExecutorConfig) (*PostgreSQLTimedExecutor, error) {
	if config.DSN == "" || config.EvidenceRoot == "" || !validSourceHash(config.AdapterVersion) || !validDigest(config.AgentBinarySHA256) {
		return nil, invalid("PostgreSQL timed executor configuration")
	}
	poolConfig, err := pgxpool.ParseConfig(config.DSN)
	if err != nil {
		return nil, err
	}
	poolConfig.MaxConns = 12
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, err
	}
	executor := &PostgreSQLTimedExecutor{config: config, pool: pool}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if _, err := pool.Exec(ctx, postgresPublicationSchema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate PostgreSQL publication schema: %w", err)
	}
	if err := pool.QueryRow(ctx, "SHOW server_version").Scan(&executor.serverVersion); err != nil {
		pool.Close()
		return nil, err
	}
	return executor, nil
}

func (e *PostgreSQLTimedExecutor) SystemID() string { return SystemPostgreSQL }

func (e *PostgreSQLTimedExecutor) Ready(ctx context.Context) error {
	e.mu.Lock()
	active, closed := e.active, e.closed
	e.mu.Unlock()
	if active != 0 || closed {
		return fmt.Errorf("PostgreSQL publication executor is not idle: closed=%t active=%d", closed, active)
	}
	if err := e.pool.Ping(ctx); err != nil {
		return err
	}
	var running int
	if err := e.pool.QueryRow(ctx, "SELECT count(*) FROM adl_publication.runs WHERE state='running'").Scan(&running); err != nil {
		return err
	}
	if running != 0 {
		return fmt.Errorf("PostgreSQL publication queue has %d unfinished runs", running)
	}
	return nil
}

func (e *PostgreSQLTimedExecutor) ExecuteTimed(ctx context.Context, request EpisodeRequest, timing *TimingRecorder) (TimedResult, error) {
	e.mu.Lock()
	if e.closed || e.active != 0 {
		e.mu.Unlock()
		return TimedResult{}, invalid("PostgreSQL timed execution state")
	}
	e.active++
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		e.active--
		e.mu.Unlock()
	}()
	plan, err := BuildEpisodePlan(request)
	if err != nil {
		return TimedResult{}, err
	}
	episode, err := NewEpisodeRuntime(EpisodeRuntimeConfig{
		Request: request, Plan: plan, SystemID: e.SystemID(), AdapterID: "postgresql-publication-v1",
		AdapterVersion: e.config.AdapterVersion, AgentBinarySHA256: e.config.AgentBinarySHA256,
		Clock: wallClock{}, Timing: timing, Settings: map[string]string{
			"postgresql_server": e.serverVersion, "claim": "for-update-skip-locked",
			"lease_fencing": "owner-generation-capability", "outbox": "transactional-acknowledgement", "worker_concurrency": "8",
		},
	})
	if err != nil {
		return TimedResult{}, err
	}
	if err := e.insertRun(ctx, episode.runID, request); err != nil {
		return TimedResult{ExecutionID: episode.runID}, err
	}
	if request.Case == protocol.CaseABAReacquisition && request.Probe != protocol.ProbeUnfaulted {
		err = e.runABA(ctx, episode, request.Probe)
	} else {
		err = e.runRounds(ctx, episode, plan)
	}
	if err != nil {
		return TimedResult{ExecutionID: episode.runID}, err
	}
	if err := e.finishRun(ctx, episode.runID); err != nil {
		return TimedResult{ExecutionID: episode.runID}, err
	}
	episode.Acknowledge()
	native, err := e.nativeJournal(ctx, episode.runID)
	if err != nil {
		return TimedResult{ExecutionID: episode.runID}, err
	}
	result, err := episode.Finish(ctx, e.config.EvidenceRoot, native)
	result.ExecutionID = episode.runID
	return result, err
}

func (e *PostgreSQLTimedExecutor) Close() error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()
	e.pool.Close()
	return nil
}

func (e *PostgreSQLTimedExecutor) insertRun(ctx context.Context, executionKey string, request EpisodeRequest) error {
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `INSERT INTO adl_publication.runs(execution_key,case_id,probe,trial,state) VALUES ($1,$2,$3,$4,'running')`,
		executionKey, string(request.Case), string(request.Probe), request.Slot); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO adl_publication.journal(execution_key,kind,detail)
VALUES ($1,'run_started',jsonb_build_object('case',$2::text,'probe',$3::text,'trial',$4::integer))`,
		executionKey, string(request.Case), string(request.Probe), request.Slot); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (e *PostgreSQLTimedExecutor) runRounds(ctx context.Context, episode *EpisodeRuntime, plan EpisodePlan) error {
	for _, round := range plan.Rounds {
		if err := e.enqueueRound(ctx, episode.runID, round); err != nil {
			return err
		}
		if err := e.runRound(ctx, episode, round); err != nil {
			return err
		}
	}
	return nil
}

func (e *PostgreSQLTimedExecutor) enqueueRound(ctx context.Context, executionKey string, round WorkRound) error {
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, work := range round.Work {
		data, err := json.Marshal(work)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO adl_publication.jobs(execution_key,round_sequence,work_id,input,state)
VALUES ($1,$2,$3,$4::jsonb,'queued')`, executionKey, round.Sequence, work.ID, data); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO adl_publication.journal(execution_key,kind,detail)
VALUES ($1,'round_enqueued',jsonb_build_object('round',$2::integer,'work_count',$3::integer))`, executionKey, round.Sequence, len(round.Work)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (e *PostgreSQLTimedExecutor) runRound(ctx context.Context, episode *EpisodeRuntime, round WorkRound) error {
	workByID := make(map[string]WorkSpec, len(round.Work))
	for _, work := range round.Work {
		workByID[work.ID] = work
	}
	gate := make(chan struct{})
	capacity := make(chan struct{}, 8)
	errorsByWork := make(chan error, len(round.Work))
	var wait sync.WaitGroup
	for index, launcher := range round.Work {
		index, launcher := index, launcher
		wait.Add(1)
		go func() {
			defer wait.Done()
			select {
			case <-ctx.Done():
				errorsByWork <- ctx.Err()
				return
			case <-gate:
			}
			if launcher.DelayMillis > 0 {
				timer := time.NewTimer(time.Duration(launcher.DelayMillis) * time.Millisecond)
				select {
				case <-ctx.Done():
					if !timer.Stop() {
						<-timer.C
					}
					errorsByWork <- ctx.Err()
					return
				case <-timer.C:
				}
			}
			select {
			case <-ctx.Done():
				errorsByWork <- ctx.Err()
				return
			case capacity <- struct{}{}:
			}
			defer func() { <-capacity }()
			owner := fmt.Sprintf("postgres-worker-%d", index%8+1)
			workID, generation, err := e.claimAny(ctx, episode.runID, round.Sequence, owner)
			if err != nil {
				errorsByWork <- err
				return
			}
			work, ok := workByID[workID]
			if !ok {
				errorsByWork <- invalid("PostgreSQL claimed unknown work")
				return
			}
			identity := NativeIdentity{WorkerID: owner, ProcessIdentity: fmt.Sprintf("pid:%d:round:%d:worker:%d", os.Getpid(), round.Sequence, index%8+1)}
			if err := episode.RunWork(ctx, work, identity); err != nil {
				_ = e.failJob(ctx, episode.runID, round.Sequence, workID, owner, generation, err)
				errorsByWork <- err
				return
			}
			errorsByWork <- e.completeJob(ctx, episode.runID, round.Sequence, workID, owner, generation)
		}()
	}
	close(gate)
	wait.Wait()
	close(errorsByWork)
	for err := range errorsByWork {
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *PostgreSQLTimedExecutor) claimAny(ctx context.Context, executionKey string, round int, owner string) (string, uint64, error) {
	var workID string
	var generation uint64
	err := e.pool.QueryRow(ctx, `WITH candidate AS (
  SELECT execution_key,round_sequence,work_id
  FROM adl_publication.jobs
  WHERE execution_key=$1 AND round_sequence=$2 AND state='queued'
  ORDER BY work_id
  FOR UPDATE SKIP LOCKED
  LIMIT 1
), claimed AS (
  UPDATE adl_publication.jobs AS job
  SET state='running', lease_owner=$3, generation=job.generation+1, started_at=clock_timestamp()
  FROM candidate
  WHERE job.execution_key=candidate.execution_key AND job.round_sequence=candidate.round_sequence AND job.work_id=candidate.work_id
  RETURNING job.work_id,job.generation
)
SELECT work_id,generation FROM claimed`, executionKey, round, owner).Scan(&workID, &generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", 0, invalid("PostgreSQL queue exhausted before cohort completion")
	}
	if err != nil {
		return "", 0, err
	}
	_, err = e.pool.Exec(ctx, `INSERT INTO adl_publication.journal(execution_key,kind,detail)
VALUES ($1,'job_claimed',jsonb_build_object('round',$2::integer,'work_id',$3::text,'owner',$4::text,'generation',$5::bigint))`,
		executionKey, round, workID, owner, generation)
	return workID, generation, err
}

func (e *PostgreSQLTimedExecutor) completeJob(ctx context.Context, executionKey string, round int, workID, owner string, generation uint64) error {
	tag, err := e.pool.Exec(ctx, `UPDATE adl_publication.jobs SET state='completed',completed_at=clock_timestamp()
WHERE execution_key=$1 AND round_sequence=$2 AND work_id=$3 AND state='running' AND lease_owner=$4 AND generation=$5`,
		executionKey, round, workID, owner, generation)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return invalid("PostgreSQL stale job completion")
	}
	_, err = e.pool.Exec(ctx, `INSERT INTO adl_publication.journal(execution_key,kind,detail)
VALUES ($1,'job_completed',jsonb_build_object('round',$2::integer,'work_id',$3::text,'owner',$4::text,'generation',$5::bigint))`,
		executionKey, round, workID, owner, generation)
	return err
}

func (e *PostgreSQLTimedExecutor) failJob(ctx context.Context, executionKey string, round int, workID, owner string, generation uint64, cause error) error {
	_, err := e.pool.Exec(ctx, `UPDATE adl_publication.jobs SET state='failed',completed_at=clock_timestamp()
WHERE execution_key=$1 AND round_sequence=$2 AND work_id=$3 AND state='running' AND lease_owner=$4 AND generation=$5`,
		executionKey, round, workID, owner, generation)
	if err != nil {
		return err
	}
	_, err = e.pool.Exec(ctx, `INSERT INTO adl_publication.journal(execution_key,kind,detail)
VALUES ($1,'job_failed',jsonb_build_object('round',$2::integer,'work_id',$3::text,'error',$4::text))`, executionKey, round, workID, cause.Error())
	return err
}

func (e *PostgreSQLTimedExecutor) runABA(ctx context.Context, episode *EpisodeRuntime, probe protocol.Probe) error {
	abaCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cap7 := digest(fmt.Sprintf("capability/%s/%d", episode.runID, 7))
	if _, err := e.pool.Exec(ctx, `INSERT INTO adl_publication.authority(execution_key,owner_id,generation,capability_hash)
VALUES ($1,'A',7,$2)`, episode.runID, cap7); err != nil {
		return err
	}
	if _, err := e.pool.Exec(ctx, `INSERT INTO adl_publication.journal(execution_key,kind,detail)
VALUES ($1,'owner_claimed',jsonb_build_object('owner','A','generation',7,'capability_hash',$2::text))`, episode.runID, cap7); err != nil {
		return err
	}
	staleDone := make(chan error, 1)
	go func() {
		staleDone <- episode.BeginABA(abaCtx, NativeIdentity{WorkerID: "postgres-worker-A7", ProcessIdentity: fmt.Sprintf("pid:%d:owner:A:generation:7", os.Getpid())})
	}()
	if err := episode.WaitABABarrier(ctx); err != nil {
		return err
	}
	for _, generation := range []uint64{8, 9} {
		owner := map[uint64]string{8: "B", 9: "A"}[generation]
		capability := digest(fmt.Sprintf("capability/%s/%d", episode.runID, generation))
		tag, err := e.pool.Exec(ctx, `UPDATE adl_publication.authority
SET owner_id=$2,generation=$3,capability_hash=$4,updated_at=clock_timestamp()
WHERE execution_key=$1 AND generation=$5`, episode.runID, owner, generation, capability, generation-1)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return invalid("PostgreSQL ABA generation transition")
		}
		if _, err := e.pool.Exec(ctx, `INSERT INTO adl_publication.journal(execution_key,kind,detail)
VALUES ($1,'owner_changed',jsonb_build_object('owner',$2::text,'generation',$3::bigint,'capability_hash',$4::text))`, episode.runID, owner, generation, capability); err != nil {
			return err
		}
		if err := episode.AdvanceABA(generation, NativeIdentity{
			WorkerID:        "postgres-worker-" + owner + strconv.FormatUint(generation, 10),
			ProcessIdentity: fmt.Sprintf("pid:%d:owner:%s:generation:%d", os.Getpid(), owner, generation),
		}); err != nil {
			return err
		}
	}
	if err := episode.CompleteABACurrent(ctx); err != nil {
		return err
	}
	if _, err := e.pool.Exec(ctx, `INSERT INTO adl_publication.journal(execution_key,kind,detail)
VALUES ($1,'current_effect_committed',jsonb_build_object('owner','A','generation',9))`, episode.runID); err != nil {
		return err
	}
	var tag pgconnCommandTag
	var err error
	if probe == protocol.ProbeUnsafe {
		tag, err = e.execTag(ctx, `UPDATE adl_publication.authority SET stopped=true,updated_at=clock_timestamp()
WHERE execution_key=$1 AND owner_id='A'`, episode.runID)
	} else {
		tag, err = e.execTag(ctx, `UPDATE adl_publication.authority SET stopped=true,updated_at=clock_timestamp()
WHERE execution_key=$1 AND owner_id='A' AND generation=7 AND capability_hash=$2`, episode.runID, cap7)
	}
	if err != nil {
		return err
	}
	staleAccepted := tag.RowsAffected() == 1
	if staleAccepted != (probe == protocol.ProbeUnsafe) {
		return invalid("PostgreSQL ABA negative-control distinction")
	}
	if _, err := e.pool.Exec(ctx, `INSERT INTO adl_publication.journal(execution_key,kind,detail)
VALUES ($1,'stale_generation_7_result',jsonb_build_object('accepted',$2::boolean,'current_generation',9))`, episode.runID, staleAccepted); err != nil {
		return err
	}
	episode.ReleaseABA(staleAccepted)
	return <-staleDone
}

type pgconnCommandTag interface{ RowsAffected() int64 }

func (e *PostgreSQLTimedExecutor) execTag(ctx context.Context, sql string, arguments ...any) (pgconnCommandTag, error) {
	return e.pool.Exec(ctx, sql, arguments...)
}

func (e *PostgreSQLTimedExecutor) finishRun(ctx context.Context, executionKey string) error {
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var completed int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM adl_publication.jobs WHERE execution_key=$1 AND state='completed'`, executionKey).Scan(&completed); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO adl_publication.outcomes(execution_key,acknowledged,completed_work) VALUES ($1,true,$2)`, executionKey, completed); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE adl_publication.runs SET state='completed',completed_at=clock_timestamp() WHERE execution_key=$1 AND state='running'`, executionKey); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO adl_publication.journal(execution_key,kind,detail)
VALUES ($1,'outcome_acknowledged',jsonb_build_object('completed_work',$2::integer))`, executionKey, completed); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (e *PostgreSQLTimedExecutor) nativeJournal(ctx context.Context, executionKey string) ([]protocol.NativeRecord, error) {
	rows, err := e.pool.Query(ctx, `SELECT event_time,kind,detail::text
FROM adl_publication.journal WHERE execution_key=$1 ORDER BY journal_id`, executionKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []protocol.NativeRecord
	for rows.Next() {
		var at time.Time
		var kind, detail string
		if err := rows.Scan(&at, &kind, &detail); err != nil {
			return nil, err
		}
		records = append(records, nativeRecord(len(records)+1, kind, detail, at))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, invalid("PostgreSQL publication journal")
	}
	return records, nil
}
