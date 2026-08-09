package postgresadapter

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/protocol"
	"github.com/sjarmak/temporal_projects/benchmarks/agent-durability/v2/systemplan"
)

const SchemaSQL = `
CREATE SCHEMA IF NOT EXISTS adl_v2;

CREATE TABLE IF NOT EXISTS adl_v2.runs (
  run_id text PRIMARY KEY,
  case_id text NOT NULL,
  probe text NOT NULL,
  trial integer NOT NULL CHECK (trial > 0),
  state text NOT NULL CHECK (state IN ('running', 'completed')),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  completed_at timestamptz
);

CREATE TABLE IF NOT EXISTS adl_v2.steps (
  run_id text NOT NULL REFERENCES adl_v2.runs(run_id),
  sequence integer NOT NULL CHECK (sequence > 0),
  step_id text NOT NULL,
  kind text NOT NULL,
  failure_once boolean NOT NULL,
  state text NOT NULL CHECK (state IN ('queued', 'running', 'completed')),
  generation bigint NOT NULL DEFAULT 0 CHECK (generation >= 0),
  lease_owner text,
  lease_until timestamptz,
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  completed_at timestamptz,
  PRIMARY KEY (run_id, sequence),
  UNIQUE (run_id, step_id)
);

CREATE TABLE IF NOT EXISTS adl_v2.journal (
  journal_id bigserial PRIMARY KEY,
  run_id text NOT NULL REFERENCES adl_v2.runs(run_id),
  event_time timestamptz NOT NULL DEFAULT clock_timestamp(),
  kind text NOT NULL,
  detail jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS adl_v2.outcomes (
  run_id text PRIMARY KEY REFERENCES adl_v2.runs(run_id),
  acknowledged boolean NOT NULL,
  completed_steps integer NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE OR REPLACE FUNCTION adl_v2.claim_step(p_run_id text, p_sequence integer, p_owner text)
RETURNS TABLE(generation bigint, attempts integer)
LANGUAGE plpgsql AS $$
DECLARE
  claimed_generation bigint;
  claimed_attempts integer;
BEGIN
  WITH candidate AS (
    SELECT queued.run_id, queued.sequence, queued.generation
    FROM adl_v2.steps AS queued
    WHERE queued.run_id = p_run_id
      AND queued.sequence = p_sequence
      AND (queued.state = 'queued' OR (queued.state = 'running' AND queued.lease_until < clock_timestamp()))
    FOR UPDATE SKIP LOCKED
  )
  UPDATE adl_v2.steps AS step
  SET state = 'running',
      lease_owner = p_owner,
      lease_until = clock_timestamp() + interval '1 minute',
      generation = candidate.generation + 1,
      attempts = step.attempts + 1
  FROM candidate
  WHERE step.run_id = candidate.run_id AND step.sequence = candidate.sequence
  RETURNING step.generation, step.attempts INTO claimed_generation, claimed_attempts;

  IF claimed_generation IS NULL THEN
    RAISE EXCEPTION 'step unavailable for claim: %/%', p_run_id, p_sequence;
  END IF;
  INSERT INTO adl_v2.journal(run_id, kind, detail)
  VALUES (p_run_id, 'step_claimed', jsonb_build_object('sequence', p_sequence, 'owner', p_owner,
          'generation', claimed_generation, 'attempts', claimed_attempts));
  generation := claimed_generation;
  attempts := claimed_attempts;
  RETURN NEXT;
END;
$$;

CREATE OR REPLACE FUNCTION adl_v2.expire_step(p_run_id text, p_sequence integer, p_owner text, p_generation bigint)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  UPDATE adl_v2.steps
  SET lease_until = clock_timestamp() - interval '1 second'
  WHERE run_id = p_run_id AND sequence = p_sequence AND state = 'running'
    AND lease_owner = p_owner AND generation = p_generation;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'stale expiry rejected: %/%', p_run_id, p_sequence;
  END IF;
  INSERT INTO adl_v2.journal(run_id, kind, detail)
  VALUES (p_run_id, 'lease_expired', jsonb_build_object('sequence', p_sequence, 'owner', p_owner,
          'generation', p_generation));
END;
$$;

CREATE OR REPLACE FUNCTION adl_v2.complete_step(p_run_id text, p_sequence integer, p_owner text, p_generation bigint)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  UPDATE adl_v2.steps
  SET state = 'completed', completed_at = clock_timestamp(), lease_until = NULL
  WHERE run_id = p_run_id AND sequence = p_sequence AND state = 'running'
    AND lease_owner = p_owner AND generation = p_generation;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'stale completion rejected: %/%', p_run_id, p_sequence;
  END IF;
  INSERT INTO adl_v2.journal(run_id, kind, detail)
  VALUES (p_run_id, 'step_completed', jsonb_build_object('sequence', p_sequence, 'owner', p_owner,
          'generation', p_generation));
END;
$$;
`

type Config struct {
	DSN            string
	PSQLPath       string
	AdapterVersion string
}

type Receipt struct {
	Sequence   int
	Generation uint64
	Attempts   int
}

type Execution = systemplan.Execution

type Session struct {
	config        Config
	psqlPath      string
	serverVersion string
	migrationHash string
}

func Open(ctx context.Context, config Config) (*Session, error) {
	if config.DSN == "" || !validSourceVersion(config.AdapterVersion) {
		return nil, fmt.Errorf("%w: PostgreSQL DSN and source adapter hash are required", protocol.ErrInvalidEvidence)
	}
	psqlPath := config.PSQLPath
	if psqlPath == "" {
		var err error
		psqlPath, err = exec.LookPath("psql")
		if err != nil {
			return nil, err
		}
	}
	session := &Session{config: config, psqlPath: psqlPath}
	if _, err := session.query(ctx, "SELECT 1"); err != nil {
		return nil, fmt.Errorf("connect PostgreSQL v2 adapter: %w", err)
	}
	if err := session.exec(ctx, SchemaSQL); err != nil {
		return nil, fmt.Errorf("migrate PostgreSQL v2 adapter: %w", err)
	}
	version, err := session.query(ctx, "SHOW server_version")
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256([]byte(SchemaSQL))
	session.serverVersion = strings.TrimSpace(version)
	session.migrationHash = hex.EncodeToString(hash[:])
	return session, nil
}

func (s *Session) Execute(ctx context.Context, plan systemplan.Plan) (Execution, error) {
	if err := plan.Validate(); err != nil {
		return Execution{}, err
	}
	runID := fmt.Sprintf("adl-v2-%s-%s-%d-%s", plan.Case, plan.Probe, plan.Trial, randomSuffix())
	if err := s.insertPlan(ctx, runID, plan); err != nil {
		return Execution{}, err
	}
	receipts := make([]Receipt, 0, len(plan.Steps))
	for _, step := range plan.Steps {
		owner := "worker-primary"
		receipt, err := s.claim(ctx, runID, step.Sequence, owner)
		if err != nil {
			return Execution{}, err
		}
		if step.FailureOnce {
			if err := s.expire(ctx, runID, receipt, owner); err != nil {
				return Execution{}, err
			}
			owner = "worker-recovery"
			receipt, err = s.claim(ctx, runID, step.Sequence, owner)
			if err != nil {
				return Execution{}, err
			}
		}
		if err := s.complete(ctx, runID, receipt, owner); err != nil {
			return Execution{}, err
		}
		receipts = append(receipts, receipt)
	}
	if err := s.finishRun(ctx, runID, len(plan.Steps)); err != nil {
		return Execution{}, err
	}
	if err := validateReceipts(plan, receipts); err != nil {
		return Execution{}, err
	}
	native, err := s.journal(ctx, runID)
	if err != nil {
		return Execution{}, err
	}
	return Execution{
		SystemID: "postgresql-queue", AdapterID: "postgresql-queue-v2", AdapterVersion: s.config.AdapterVersion,
		ExecutionID: runID, Native: native,
		Settings: map[string]string{
			"track": track(plan.Probe), "postgresql_server": s.serverVersion, "migration_sha256": s.migrationHash,
			"claim": "for-update-skip-locked", "lease_recovery": "generation-checked-expiry", "outbox": "transactional-outcome-row",
		},
	}, nil
}

func (s *Session) insertPlan(ctx context.Context, runID string, plan systemplan.Plan) error {
	var statement strings.Builder
	fmt.Fprintf(&statement, "BEGIN; INSERT INTO adl_v2.runs(run_id,case_id,probe,trial,state) VALUES (%s,%s,%s,%d,'running');",
		sqlLiteral(runID), sqlLiteral(string(plan.Case)), sqlLiteral(string(plan.Probe)), plan.Trial)
	for _, step := range plan.Steps {
		fmt.Fprintf(&statement, "INSERT INTO adl_v2.steps(run_id,sequence,step_id,kind,failure_once,state) VALUES (%s,%d,%s,%s,%t,'queued');",
			sqlLiteral(runID), step.Sequence, sqlLiteral(step.ID), sqlLiteral(step.Kind), step.FailureOnce)
	}
	fmt.Fprintf(&statement, "INSERT INTO adl_v2.journal(run_id,kind,detail) VALUES (%s,'plan_enqueued',jsonb_build_object('steps',%d)); COMMIT;", sqlLiteral(runID), len(plan.Steps))
	return s.exec(ctx, statement.String())
}

func (s *Session) claim(ctx context.Context, runID string, sequence int, owner string) (Receipt, error) {
	value, err := s.query(ctx, fmt.Sprintf("SELECT generation, attempts FROM adl_v2.claim_step(%s,%d,%s)", sqlLiteral(runID), sequence, sqlLiteral(owner)))
	if err != nil {
		return Receipt{}, err
	}
	fields := strings.Split(strings.TrimSpace(value), "\t")
	if len(fields) != 2 {
		return Receipt{}, fmt.Errorf("%w: PostgreSQL claim receipt %q", protocol.ErrInvalidEvidence, value)
	}
	generation, generationErr := strconv.ParseUint(fields[0], 10, 64)
	attempts, attemptsErr := strconv.Atoi(fields[1])
	if generationErr != nil || attemptsErr != nil {
		return Receipt{}, fmt.Errorf("%w: malformed PostgreSQL claim receipt", protocol.ErrInvalidEvidence)
	}
	return Receipt{Sequence: sequence, Generation: generation, Attempts: attempts}, nil
}

func (s *Session) expire(ctx context.Context, runID string, receipt Receipt, owner string) error {
	return s.exec(ctx, fmt.Sprintf("SELECT adl_v2.expire_step(%s,%d,%s,%d)", sqlLiteral(runID), receipt.Sequence, sqlLiteral(owner), receipt.Generation))
}

func (s *Session) complete(ctx context.Context, runID string, receipt Receipt, owner string) error {
	return s.exec(ctx, fmt.Sprintf("SELECT adl_v2.complete_step(%s,%d,%s,%d)", sqlLiteral(runID), receipt.Sequence, sqlLiteral(owner), receipt.Generation))
}

func (s *Session) finishRun(ctx context.Context, runID string, count int) error {
	statement := fmt.Sprintf(`BEGIN;
INSERT INTO adl_v2.outcomes(run_id,acknowledged,completed_steps)
SELECT %s, true, count(*) FROM adl_v2.steps WHERE run_id=%s AND state='completed'
HAVING count(*)=%d;
UPDATE adl_v2.runs SET state='completed', completed_at=clock_timestamp() WHERE run_id=%s
AND EXISTS (SELECT 1 FROM adl_v2.outcomes WHERE run_id=%s AND acknowledged);
INSERT INTO adl_v2.journal(run_id,kind,detail) VALUES (%s,'outcome_acknowledged',jsonb_build_object('completed_steps',%d));
COMMIT;`, sqlLiteral(runID), sqlLiteral(runID), count, sqlLiteral(runID), sqlLiteral(runID), sqlLiteral(runID), count)
	if err := s.exec(ctx, statement); err != nil {
		return err
	}
	state, err := s.query(ctx, fmt.Sprintf("SELECT state FROM adl_v2.runs WHERE run_id=%s", sqlLiteral(runID)))
	if err != nil || strings.TrimSpace(state) != "completed" {
		return fmt.Errorf("%w: PostgreSQL run did not reach completed state: %v", protocol.ErrInvalidEvidence, err)
	}
	return nil
}

func (s *Session) journal(ctx context.Context, runID string) ([]protocol.NativeRecord, error) {
	query := fmt.Sprintf(`SELECT to_char(event_time AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'), kind, detail::text
FROM adl_v2.journal WHERE run_id=%s ORDER BY journal_id`, sqlLiteral(runID))
	output, err := s.query(ctx, query)
	if err != nil {
		return nil, err
	}
	var records []protocol.NativeRecord
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		fields := strings.SplitN(line, "\t", 3)
		if len(fields) != 3 {
			return nil, fmt.Errorf("%w: malformed PostgreSQL journal line", protocol.ErrInvalidEvidence)
		}
		if _, err := time.Parse(time.RFC3339Nano, fields[0]); err != nil {
			return nil, err
		}
		records = append(records, protocol.NativeRecord{
			Sequence: uint64(len(records) + 1), Time: fields[0], Kind: fields[1], Detail: fields[2],
		})
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("%w: PostgreSQL journal is empty", protocol.ErrInvalidEvidence)
	}
	return records, nil
}

func (s *Session) exec(ctx context.Context, statement string) error {
	_, err := s.runPSQL(ctx, statement, false)
	return err
}

func (s *Session) query(ctx context.Context, statement string) (string, error) {
	return s.runPSQL(ctx, statement, true)
}

func (s *Session) runPSQL(ctx context.Context, statement string, query bool) (string, error) {
	arguments := []string{"--no-psqlrc", "--set=ON_ERROR_STOP=1", "--quiet", "--dbname", s.config.DSN}
	if query {
		arguments = append(arguments, "--tuples-only", "--no-align", "--field-separator", "\t")
	}
	arguments = append(arguments, "--command", statement)
	command := exec.CommandContext(ctx, s.psqlPath, arguments...)
	command.Env = append(os.Environ(), "PGCONNECT_TIMEOUT=5")
	output, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("psql command failed: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return string(output), nil
}

func validateReceipts(plan systemplan.Plan, receipts []Receipt) error {
	if len(receipts) != len(plan.Steps) {
		return fmt.Errorf("%w: PostgreSQL receipt count differs from plan", protocol.ErrInvalidEvidence)
	}
	for index, receipt := range receipts {
		want := 1
		if plan.Steps[index].FailureOnce {
			want = 2
		}
		if receipt.Sequence != plan.Steps[index].Sequence || receipt.Attempts != want || receipt.Generation != uint64(want) {
			return fmt.Errorf("%w: PostgreSQL receipt %d differs from lease plan", protocol.ErrInvalidEvidence, index)
		}
	}
	return nil
}

func expectedReceipts(plan systemplan.Plan) []Receipt {
	result := make([]Receipt, 0, len(plan.Steps))
	for _, step := range plan.Steps {
		attempts := 1
		if step.FailureOnce {
			attempts = 2
		}
		result = append(result, Receipt{Sequence: step.Sequence, Generation: uint64(attempts), Attempts: attempts})
	}
	return result
}

func sqlLiteral(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }

func validSourceVersion(value string) bool {
	encoded := strings.TrimPrefix(value, "source-sha256:")
	decoded, err := hex.DecodeString(encoded)
	return value != encoded && err == nil && len(decoded) == 32
}

func track(probe protocol.Probe) string {
	if probe == protocol.ProbeProtected {
		return "portable-safety"
	}
	return "native-minimum"
}

func randomSuffix() string {
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(value[:])
}
