package semantics

import "time"

type WorkerConfiguration struct {
	WorkActivityConcurrency   int   `json:"work_activity_concurrency"`
	EffectActivityConcurrency int   `json:"effect_activity_concurrency"`
	WorkflowTaskConcurrency   int   `json:"workflow_task_concurrency"`
	WorkerStopTimeoutMS       int64 `json:"worker_stop_timeout_ms"`
}

func FrozenWorkerConfiguration() WorkerConfiguration {
	return WorkerConfiguration{
		WorkActivityConcurrency: workerActivityConcurrency, EffectActivityConcurrency: workerActivityConcurrency,
		WorkflowTaskConcurrency: workflowTaskConcurrency, WorkerStopTimeoutMS: (100 * time.Millisecond).Milliseconds(),
	}
}
