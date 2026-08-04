// Command batch-api-go manually exercises the full batch-operation API surface
// touched by temporal PR #10803 / api PR #806 / cli PR #1100, against a live
// dev server built from that branch.
//
// Coverage:
//
//   - StartBatchOperation for every operation, each across every applicable
//     target selector:
//     workflow: terminate, cancel, signal, delete, reset, update-options
//     x {VisibilityQuery, Executions (deprecated), ArchetypeExecutions}
//     activity (SAA): terminate, cancel, delete
//     x {VisibilityQuery, ArchetypeExecutions}
//   - DescribeBatchOperation: OperationType, Query, Executions, and success
//     counts are checked for every job (this is where the *_WORKFLOW/*_ACTIVITY
//     enum, the query-echo, and the executions-echo all get exercised).
//   - ListBatchOperations: OperationType + state checked for every job.
//   - CountWorkflowExecutions / CountActivityExecutions: used as the pre-batch
//     readiness/size gate and to confirm terminate's effect.
//   - Per-target effect: DescribeWorkflowExecution / DescribeActivityExecution
//     / GetWorkflowExecutionHistory.
//   - In-progress control plane: a throttled batch (server started with a low
//     worker.batcherRPS) is observed RUNNING via Describe and List, then
//     StopBatchOperation is issued and the job is confirmed to leave RUNNING
//     (a stopped batch's batcher workflow is terminated, so Describe reports
//     FAILED). Exercised for both a workflow batch (ListWorkflow path) and an
//     SAA batch (ListActivityExecutions path).
//
// Not covered (documented, not silent): the pre-existing workflow-embedded
// activity batch ops (unpause / reset-activity / update-activity-options).
// Their memo strings are unchanged by this PR and they list *workflows*, not
// standalone activities; exercising them needs workflows carrying pending
// (and, for unpause, paused) activities. They are neither top-level workflow
// nor SAA operations, so they are out of scope here.
//
// Prerequisites:
//
//	cd ~/worktrees/cli/ks--saa-batch-cmds/cli
//	go build -o /tmp/temporal-saa-batch ./cmd/temporal
//	# The in-progress checks need the batch throttled so it is observably
//	# running; the per-request MaxOperationsPerSecond is NOT honored by the
//	# batcher (it uses worker.batcherRPS), so set it on the server:
//	/tmp/temporal-saa-batch server start-dev --headless \
//	  --dynamic-config-value worker.batcherRPS=5 \
//	  --dynamic-config-value frontend.MaxConcurrentBatchOperationPerNamespace=10
//
// Usage:
//
//	go run . [-namespace default]
//
// Env: TEMPORAL_ADDRESS (default localhost:7233), TEMPORAL_NAMESPACE (default "default").
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	workerTaskQueue   = "batch-api-workload-tq"
	saaTaskQueue      = "batch-api-workload-saa-tq" // deliberately unpolled
	blockingWFType    = "BatchAPIWorkloadBlocking"
	targetsPerCase    = 3
	inProgressTargets = 40 // large enough to stay RUNNING past the ~2.5s first-heartbeat estimate at a low batcherRPS
	activityTimeout   = 30 * time.Minute
	batchWaitTimeout  = 60 * time.Second
	pollInterval      = 200 * time.Millisecond
	identity          = "batch-api-workload"
)

// blockingWorkflow runs its first workflow task (so it has a completed
// WorkflowTask event, which reset needs) and then blocks forever, staying
// Running until a batch op acts on it. If the execution is cancelled its
// context is cancelled and Await returns, so cancel batches drive it to
// Canceled.
func blockingWorkflow(ctx workflow.Context) error {
	return workflow.Await(ctx, func() bool { return false })
}

var (
	client    sdkclient.Client
	ws        workflowservice.WorkflowServiceClient
	namespace string
)

func main() {
	nsFlag := flag.String("namespace", envOr("TEMPORAL_NAMESPACE", "default"), "namespace to run against")
	flag.Parse()
	namespace = *nsFlag

	address := envOr("TEMPORAL_ADDRESS", "localhost:7233")
	c, err := sdkclient.Dial(sdkclient.Options{HostPort: address, Namespace: namespace})
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial %s: %v\n", address, err)
		os.Exit(1)
	}
	defer c.Close()
	client = c
	ws = c.WorkflowService()

	// One worker, one workflow type for every workflow target. Cases are scoped
	// by a unique WorkflowId prefix (not by type), so the same server can be
	// reused across runs without stale executions of a shared type colliding.
	w := worker.New(c, workerTaskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(blockingWorkflow, workflow.RegisterOptions{Name: blockingWFType})
	if err := w.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "start worker: %v\n", err)
		os.Exit(1)
	}
	defer w.Stop()

	ctx := context.Background()
	failed := false
	record := func(label string, err error) {
		if err != nil {
			fmt.Printf("FAIL  %s: %v\n", label, err)
			failed = true
		} else {
			fmt.Printf("PASS  %s\n", label)
		}
	}

	fmt.Println("== workflow batch operations ==")
	for _, op := range wfOps {
		for _, mode := range wfModes {
			record(fmt.Sprintf("workflow %-14s via %-20s", op, mode),
				runWorkflowCase(ctx, op, mode))
		}
	}

	fmt.Println("== standalone-activity batch operations ==")
	for _, op := range saaOps {
		for _, mode := range saaModes {
			record(fmt.Sprintf("activity %-14s via %-20s", op, mode),
				runActivityCase(ctx, op, mode))
		}
	}

	fmt.Println("== in-progress control plane (describe/list while running, then stop) ==")
	record("workflow in-progress + stop", runInProgress(ctx, execWorkflow))
	record("activity in-progress + stop", runInProgress(ctx, execActivity))

	if failed {
		os.Exit(1)
	}
}

// ---- operation / mode enumeration ---------------------------------------

var (
	wfOps    = []string{"terminate", "cancel", "signal", "delete", "reset", "update-options"}
	wfModes  = []targetMode{modeQuery, modeExecutions, modeArchetype}
	saaOps   = []string{"terminate", "cancel", "delete"}
	saaModes = []targetMode{modeQuery, modeArchetype}
)

type targetMode int

const (
	modeQuery targetMode = iota
	modeExecutions
	modeArchetype
)

func (m targetMode) String() string {
	switch m {
	case modeQuery:
		return "query"
	case modeExecutions:
		return "executions"
	case modeArchetype:
		return "archetype-executions"
	default:
		return "?"
	}
}

type execType int

const (
	execWorkflow execType = iota
	execActivity
)

// ---- workflow cases ------------------------------------------------------

func runWorkflowCase(ctx context.Context, op string, mode targetMode) error {
	unique := fmt.Sprintf("wf-%s-%d-%s", op, mode, uuid.NewString()[:8])

	execs := make([]*commonpb.WorkflowExecution, 0, targetsPerCase)
	for i := 0; i < targetsPerCase; i++ {
		id := fmt.Sprintf("%s-%d", unique, i)
		run, err := client.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			ID:        id,
			TaskQueue: workerTaskQueue,
		}, blockingWFType)
		if err != nil {
			return fmt.Errorf("start workflow %s: %w", id, err)
		}
		execs = append(execs, &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()})
	}

	// Every workflow must have completed its first workflow task before we act:
	// reset needs a completed WorkflowTask to reset to, and it also confirms the
	// worker is really running these.
	for _, e := range execs {
		if err := waitForFirstWFTCompleted(ctx, e.GetWorkflowId()); err != nil {
			return err
		}
	}

	// Scope by WorkflowId prefix, not type, so reusing the server across runs
	// doesn't accumulate matches.
	query := fmt.Sprintf("WorkflowId STARTS_WITH '%s'", unique)
	if mode == modeQuery {
		if err := waitForWorkflowCount(ctx, query, targetsPerCase); err != nil {
			return fmt.Errorf("pre-batch count: %w", err)
		}
	}

	jobID := uuid.NewString()
	req := &workflowservice.StartBatchOperationRequest{
		Namespace: namespace,
		JobId:     jobID,
		Reason:    "batch-api-workload",
	}
	setWorkflowOperation(req, op)
	wantQuery, wantExecs := applyWorkflowTargetMode(req, mode, query, execs)

	if _, err := ws.StartBatchOperation(ctx, req); err != nil {
		return fmt.Errorf("StartBatchOperation: %w", err)
	}

	if err := verifyBatchCompleted(ctx, jobID, wfBatchType(op), wantQuery, wantExecs, targetsPerCase); err != nil {
		return err
	}

	for _, e := range execs {
		if err := verifyWorkflowTarget(ctx, op, e); err != nil {
			return err
		}
	}

	// Count reflects terminate's effect (targets leave Running).
	if op == "terminate" {
		if err := waitForWorkflowCount(ctx, query+" AND ExecutionStatus = 'Running'", 0); err != nil {
			return fmt.Errorf("post-terminate running count: %w", err)
		}
	}
	return nil
}

func setWorkflowOperation(req *workflowservice.StartBatchOperationRequest, op string) {
	switch op {
	case "terminate":
		req.Operation = &workflowservice.StartBatchOperationRequest_TerminationOperation{
			TerminationOperation: &batchpb.BatchOperationTermination{Identity: identity},
		}
	case "cancel":
		req.Operation = &workflowservice.StartBatchOperationRequest_CancellationOperation{
			CancellationOperation: &batchpb.BatchOperationCancellation{Identity: identity},
		}
	case "signal":
		req.Operation = &workflowservice.StartBatchOperationRequest_SignalOperation{
			SignalOperation: &batchpb.BatchOperationSignal{Signal: "batch-api-signal", Identity: identity},
		}
	case "delete":
		req.Operation = &workflowservice.StartBatchOperationRequest_DeletionOperation{
			DeletionOperation: &batchpb.BatchOperationDeletion{},
		}
	case "reset":
		req.Operation = &workflowservice.StartBatchOperationRequest_ResetOperation{
			ResetOperation: &batchpb.BatchOperationReset{
				ResetType: enumspb.RESET_TYPE_FIRST_WORKFLOW_TASK,
				Identity:  identity,
			},
		}
	case "update-options":
		req.Operation = &workflowservice.StartBatchOperationRequest_UpdateWorkflowOptionsOperation{
			UpdateWorkflowOptionsOperation: &batchpb.BatchOperationUpdateWorkflowExecutionOptions{
				Identity: identity,
				WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{
					VersioningOverride: &workflowpb.VersioningOverride{
						Override: &workflowpb.VersioningOverride_AutoUpgrade{AutoUpgrade: true},
					},
				},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
			},
		}
	default:
		panic("unknown workflow op " + op)
	}
}

func wfBatchType(op string) enumspb.BatchOperationType {
	switch op {
	case "terminate":
		return enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW
	case "cancel":
		return enumspb.BATCH_OPERATION_TYPE_CANCEL_WORKFLOW
	case "signal":
		return enumspb.BATCH_OPERATION_TYPE_SIGNAL_WORKFLOW
	case "delete":
		return enumspb.BATCH_OPERATION_TYPE_DELETE_WORKFLOW
	case "reset":
		return enumspb.BATCH_OPERATION_TYPE_RESET_WORKFLOW
	case "update-options":
		return enumspb.BATCH_OPERATION_TYPE_UPDATE_WORKFLOW_EXECUTION_OPTIONS
	default:
		panic("unknown workflow op " + op)
	}
}

// applyWorkflowTargetMode sets the chosen selector on the request and returns
// what DescribeBatchOperation is expected to echo back (query, executions).
func applyWorkflowTargetMode(req *workflowservice.StartBatchOperationRequest, mode targetMode, query string, execs []*commonpb.WorkflowExecution) (string, []*commonpb.Execution) {
	switch mode {
	case modeQuery:
		req.VisibilityQuery = query
		return query, nil
	case modeExecutions:
		//nolint:staticcheck // SA1019: intentionally exercising the deprecated Executions selector
		req.Executions = execs
		return "", toArchetype(execs, enumspb.EXECUTION_TYPE_WORKFLOW)
	case modeArchetype:
		archetype := toArchetype(execs, enumspb.EXECUTION_TYPE_WORKFLOW)
		req.ArchetypeExecutions = archetype
		return "", archetype
	default:
		panic("unknown mode")
	}
}

func verifyWorkflowTarget(ctx context.Context, op string, e *commonpb.WorkflowExecution) error {
	switch op {
	case "terminate":
		return waitForWorkflowStatus(ctx, e.GetWorkflowId(), enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED)
	case "cancel":
		if err := waitForHistoryEvent(ctx, e.GetWorkflowId(), enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED); err != nil {
			return err
		}
		return waitForWorkflowStatus(ctx, e.GetWorkflowId(), enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED)
	case "signal":
		if err := waitForHistoryEvent(ctx, e.GetWorkflowId(), enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED); err != nil {
			return err
		}
		return waitForWorkflowStatus(ctx, e.GetWorkflowId(), enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
	case "delete":
		return waitForWorkflowGone(ctx, e.GetWorkflowId())
	case "reset":
		// Reset to the first workflow task starts a new run; the current run for
		// the workflow ID must therefore differ from the one we started.
		return waitUntil(func() error {
			desc, err := ws.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: namespace,
				Execution: &commonpb.WorkflowExecution{WorkflowId: e.GetWorkflowId()},
			})
			if err != nil {
				return err
			}
			if cur := desc.GetWorkflowExecutionInfo().GetExecution().GetRunId(); cur == e.GetRunId() {
				return fmt.Errorf("run id unchanged after reset (%s)", cur)
			}
			return nil
		})
	case "update-options":
		return waitUntil(func() error {
			desc, err := ws.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: namespace,
				Execution: e,
			})
			if err != nil {
				return err
			}
			ov := desc.GetWorkflowExecutionInfo().GetVersioningInfo().GetVersioningOverride().GetOverride()
			au, ok := ov.(*workflowpb.VersioningOverride_AutoUpgrade)
			if !ok || !au.AutoUpgrade {
				return fmt.Errorf("versioning override not applied")
			}
			return nil
		})
	default:
		panic("unknown workflow op " + op)
	}
}

// ---- SAA (standalone activity) cases ------------------------------------

func runActivityCase(ctx context.Context, op string, mode targetMode) error {
	activityType := fmt.Sprintf("BatchAPISAA_%s_%s_%s", op, mode, uuid.NewString()[:8])

	execs := make([]*commonpb.Execution, 0, targetsPerCase)
	for i := 0; i < targetsPerCase; i++ {
		id := fmt.Sprintf("%s-%d", activityType, i)
		runID, err := startStandaloneActivity(ctx, id, activityType)
		if err != nil {
			return fmt.Errorf("start activity %s: %w", id, err)
		}
		execs = append(execs, &commonpb.Execution{Type: enumspb.EXECUTION_TYPE_ACTIVITY, BusinessId: id, RunId: runID})
	}

	query := fmt.Sprintf("ActivityType = '%s'", activityType)
	if mode == modeQuery {
		if err := waitForActivityCount(ctx, query, targetsPerCase); err != nil {
			return fmt.Errorf("pre-batch count: %w", err)
		}
	}

	jobID := uuid.NewString()
	req := &workflowservice.StartBatchOperationRequest{
		Namespace: namespace,
		JobId:     jobID,
		Reason:    "batch-api-workload",
	}
	setActivityOperation(req, op)
	var wantQuery string
	var wantExecs []*commonpb.Execution
	switch mode {
	case modeQuery:
		req.VisibilityQuery = query
		wantQuery = query
	case modeArchetype:
		req.ArchetypeExecutions = execs
		wantExecs = execs
	default:
		return fmt.Errorf("unsupported SAA target mode %v", mode)
	}

	if _, err := ws.StartBatchOperation(ctx, req); err != nil {
		return fmt.Errorf("StartBatchOperation: %w", err)
	}

	if err := verifyBatchCompleted(ctx, jobID, saaBatchType(op), wantQuery, wantExecs, targetsPerCase); err != nil {
		return err
	}

	for _, e := range execs {
		if err := verifyActivityTarget(ctx, op, e); err != nil {
			return err
		}
	}

	if op == "terminate" || op == "cancel" || op == "delete" {
		if err := waitForActivityCount(ctx, query+" AND ExecutionStatus = 'Running'", 0); err != nil {
			return fmt.Errorf("post-%s running count: %w", op, err)
		}
	}
	return nil
}

func setActivityOperation(req *workflowservice.StartBatchOperationRequest, op string) {
	switch op {
	case "terminate":
		req.Operation = &workflowservice.StartBatchOperationRequest_TerminateActivitiesOperation{
			TerminateActivitiesOperation: &batchpb.BatchOperationTerminateActivities{Identity: identity, Reason: "batch-api-workload"},
		}
	case "cancel":
		req.Operation = &workflowservice.StartBatchOperationRequest_CancelActivitiesOperation{
			CancelActivitiesOperation: &batchpb.BatchOperationCancelActivities{Identity: identity, Reason: "batch-api-workload"},
		}
	case "delete":
		req.Operation = &workflowservice.StartBatchOperationRequest_DeleteActivitiesOperation{
			DeleteActivitiesOperation: &batchpb.BatchOperationDeleteActivities{},
		}
	default:
		panic("unknown SAA op " + op)
	}
}

func saaBatchType(op string) enumspb.BatchOperationType {
	switch op {
	case "terminate":
		return enumspb.BATCH_OPERATION_TYPE_TERMINATE_ACTIVITY
	case "cancel":
		return enumspb.BATCH_OPERATION_TYPE_CANCEL_ACTIVITY
	case "delete":
		return enumspb.BATCH_OPERATION_TYPE_DELETE_ACTIVITY
	default:
		panic("unknown SAA op " + op)
	}
}

func verifyActivityTarget(ctx context.Context, op string, e *commonpb.Execution) error {
	if op == "delete" {
		return waitForActivityGone(ctx, e.GetBusinessId(), e.GetRunId())
	}
	want := map[string]enumspb.ActivityExecutionStatus{
		"terminate": enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		"cancel":    enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED,
	}[op]
	return waitForActivityStatus(ctx, e.GetBusinessId(), e.GetRunId(), want)
}

func startStandaloneActivity(ctx context.Context, id, activityType string) (string, error) {
	resp, err := ws.StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:              namespace,
		RequestId:              uuid.NewString(),
		ActivityId:             id,
		ActivityType:           &commonpb.ActivityType{Name: activityType},
		TaskQueue:              &taskqueuepb.TaskQueue{Name: saaTaskQueue},
		ScheduleToCloseTimeout: durationpb.New(activityTimeout),
		StartToCloseTimeout:    durationpb.New(activityTimeout),
	})
	if err != nil {
		return "", err
	}
	return resp.GetRunId(), nil
}

// ---- in-progress / stop --------------------------------------------------

func runInProgress(ctx context.Context, et execType) error {
	tag := fmt.Sprintf("BatchAPIInProgress_%d_%s", et, uuid.NewString()[:8])

	var query string
	switch et {
	case execWorkflow:
		for i := 0; i < inProgressTargets; i++ {
			if _, err := client.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
				ID:        fmt.Sprintf("%s-%d", tag, i),
				TaskQueue: workerTaskQueue,
			}, blockingWFType); err != nil {
				return fmt.Errorf("start workflow: %w", err)
			}
		}
		// Distinguish this batch's targets by ID prefix so the query matches
		// only them (all share one registered workflow type).
		query = fmt.Sprintf("WorkflowId STARTS_WITH '%s' AND ExecutionStatus = 'Running'", tag)
		if err := waitForWorkflowCount(ctx, query, inProgressTargets); err != nil {
			return fmt.Errorf("pre-batch count: %w", err)
		}
	case execActivity:
		for i := 0; i < inProgressTargets; i++ {
			if _, err := startStandaloneActivity(ctx, fmt.Sprintf("%s-%d", tag, i), tag); err != nil {
				return fmt.Errorf("start activity: %w", err)
			}
		}
		query = fmt.Sprintf("ActivityType = '%s'", tag)
		if err := waitForActivityCount(ctx, query, inProgressTargets); err != nil {
			return fmt.Errorf("pre-batch count: %w", err)
		}
	}

	jobID := uuid.NewString()
	req := &workflowservice.StartBatchOperationRequest{
		Namespace:       namespace,
		JobId:           jobID,
		Reason:          "batch-api-workload in-progress",
		VisibilityQuery: query,
	}
	if et == execWorkflow {
		setWorkflowOperation(req, "terminate")
	} else {
		setActivityOperation(req, "terminate")
	}
	if _, err := ws.StartBatchOperation(ctx, req); err != nil {
		return fmt.Errorf("StartBatchOperation: %w", err)
	}

	// Observe it RUNNING via Describe and List while it is still in flight (the
	// server must be started with a low worker.batcherRPS so it lasts long
	// enough). The estimated TotalOperationCount only appears once the batcher
	// records its first heartbeat, so poll for it -- but bail out if the batch
	// leaves RUNNING before we see it (means it ran too fast to observe).
	deadline := time.Now().Add(batchWaitTimeout)
	for {
		desc, err := ws.DescribeBatchOperation(ctx, &workflowservice.DescribeBatchOperationRequest{Namespace: namespace, JobId: jobID})
		if err != nil {
			return fmt.Errorf("describe (running): %w", err)
		}
		if desc.GetState() != enumspb.BATCH_OPERATION_STATE_RUNNING {
			return fmt.Errorf("batch left RUNNING (state %v) before it could be observed in-progress; lower worker.batcherRPS", desc.GetState())
		}
		if desc.GetTotalOperationCount() == int64(inProgressTargets) {
			if c := desc.GetCompleteOperationCount(); c >= int64(inProgressTargets) {
				return fmt.Errorf("running describe: complete=%d not < total=%d", c, inProgressTargets)
			}
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("never observed estimated total=%d while RUNNING", inProgressTargets)
		}
		time.Sleep(pollInterval)
	}
	if err := findInList(ctx, jobID, enumspb.BATCH_OPERATION_STATE_RUNNING); err != nil {
		return fmt.Errorf("list (running): %w", err)
	}

	if _, err := ws.StopBatchOperation(ctx, &workflowservice.StopBatchOperationRequest{
		Namespace: namespace,
		JobId:     jobID,
		Reason:    "batch-api-workload stop",
		Identity:  identity,
	}); err != nil {
		return fmt.Errorf("StopBatchOperation: %w", err)
	}

	// A stopped batch's batcher workflow is terminated -> Describe reports FAILED.
	return waitUntil(func() error {
		d, err := ws.DescribeBatchOperation(ctx, &workflowservice.DescribeBatchOperationRequest{Namespace: namespace, JobId: jobID})
		if err != nil {
			return err
		}
		switch d.GetState() {
		case enumspb.BATCH_OPERATION_STATE_RUNNING:
			return fmt.Errorf("still RUNNING after stop")
		case enumspb.BATCH_OPERATION_STATE_FAILED:
			return nil
		default:
			return fmt.Errorf("unexpected post-stop state %v", d.GetState())
		}
	})
}

// ---- shared batch verification ------------------------------------------

func verifyBatchCompleted(ctx context.Context, jobID string, wantType enumspb.BatchOperationType, wantQuery string, wantExecs []*commonpb.Execution, wantCount int) error {
	var desc *workflowservice.DescribeBatchOperationResponse
	if err := waitUntil(func() error {
		d, err := ws.DescribeBatchOperation(ctx, &workflowservice.DescribeBatchOperationRequest{Namespace: namespace, JobId: jobID})
		if err != nil {
			return err
		}
		if d.GetState() == enumspb.BATCH_OPERATION_STATE_RUNNING {
			return fmt.Errorf("still running")
		}
		desc = d
		return nil
	}); err != nil {
		return fmt.Errorf("describe: %w", err)
	}

	if desc.GetState() != enumspb.BATCH_OPERATION_STATE_COMPLETED {
		return fmt.Errorf("state = %v, want COMPLETED", desc.GetState())
	}
	if desc.GetOperationType() != wantType {
		return fmt.Errorf("operationType = %v, want %v", desc.GetOperationType(), wantType)
	}
	if desc.GetQuery() != wantQuery {
		return fmt.Errorf("query = %q, want %q", desc.GetQuery(), wantQuery)
	}
	if err := executionsEqual(wantExecs, desc.GetExecutions()); err != nil {
		return fmt.Errorf("describe executions: %w", err)
	}
	if got := desc.GetCompleteOperationCount(); got != int64(wantCount) {
		return fmt.Errorf("completeCount = %d, want %d", got, wantCount)
	}
	if got := desc.GetFailureOperationCount(); got != 0 {
		return fmt.Errorf("failureCount = %d, want 0", got)
	}
	return findInList(ctx, jobID, enumspb.BATCH_OPERATION_STATE_COMPLETED, wantType)
}

// findInList confirms the job appears in ListBatchOperations with the expected
// state and (if given) operation type.
func findInList(ctx context.Context, jobID string, wantState enumspb.BatchOperationState, wantType ...enumspb.BatchOperationType) error {
	return waitUntil(func() error {
		resp, err := ws.ListBatchOperations(ctx, &workflowservice.ListBatchOperationsRequest{Namespace: namespace, PageSize: 1000})
		if err != nil {
			return err
		}
		for _, info := range resp.GetOperationInfo() {
			if info.GetJobId() != jobID {
				continue
			}
			if info.GetState() != wantState {
				return fmt.Errorf("list state = %v, want %v", info.GetState(), wantState)
			}
			if len(wantType) > 0 && info.GetOperationType() != wantType[0] {
				return fmt.Errorf("list operationType = %v, want %v", info.GetOperationType(), wantType[0])
			}
			return nil
		}
		return fmt.Errorf("job %s not in ListBatchOperations", jobID)
	})
}

// ---- small helpers -------------------------------------------------------

func toArchetype(execs []*commonpb.WorkflowExecution, t enumspb.ExecutionType) []*commonpb.Execution {
	out := make([]*commonpb.Execution, len(execs))
	for i, e := range execs {
		out[i] = &commonpb.Execution{Type: t, BusinessId: e.GetWorkflowId(), RunId: e.GetRunId()}
	}
	return out
}

// executionsEqual compares by (type, businessID, runID) as a set, ignoring
// order and proto-internal fields.
func executionsEqual(want, got []*commonpb.Execution) error {
	if len(want) != len(got) {
		return fmt.Errorf("got %d executions, want %d", len(got), len(want))
	}
	key := func(e *commonpb.Execution) string {
		return fmt.Sprintf("%v|%s|%s", e.GetType(), e.GetBusinessId(), e.GetRunId())
	}
	seen := map[string]bool{}
	for _, e := range got {
		seen[key(e)] = true
	}
	for _, e := range want {
		if !seen[key(e)] {
			return fmt.Errorf("missing execution %s", key(e))
		}
	}
	return nil
}

func waitForFirstWFTCompleted(ctx context.Context, workflowID string) error {
	return waitForHistoryEvent(ctx, workflowID, enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED)
}

func waitForHistoryEvent(ctx context.Context, workflowID string, want enumspb.EventType) error {
	return waitUntil(func() error {
		resp, err := ws.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: namespace,
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		})
		if err != nil {
			return err
		}
		for _, ev := range resp.GetHistory().GetEvents() {
			if ev.GetEventType() == want {
				return nil
			}
		}
		return fmt.Errorf("event %v not yet in history of %s", want, workflowID)
	})
}

func waitForWorkflowStatus(ctx context.Context, workflowID string, want enumspb.WorkflowExecutionStatus) error {
	return waitUntil(func() error {
		desc, err := ws.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: namespace,
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		})
		if err != nil {
			return err
		}
		if got := desc.GetWorkflowExecutionInfo().GetStatus(); got != want {
			return fmt.Errorf("workflow %s status = %v, want %v", workflowID, got, want)
		}
		return nil
	})
}

func waitForWorkflowGone(ctx context.Context, workflowID string) error {
	return waitUntil(func() error {
		_, err := ws.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: namespace,
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		})
		var nf *serviceerror.NotFound
		if errors.As(err, &nf) {
			return nil
		}
		if err != nil {
			return err
		}
		return fmt.Errorf("workflow %s still exists after delete", workflowID)
	})
}

func waitForActivityStatus(ctx context.Context, activityID, runID string, want enumspb.ActivityExecutionStatus) error {
	return waitUntil(func() error {
		desc, err := ws.DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  namespace,
			ActivityId: activityID,
			RunId:      runID,
		})
		if err != nil {
			return err
		}
		if got := desc.GetInfo().GetStatus(); got != want {
			return fmt.Errorf("activity %s status = %v, want %v", activityID, got, want)
		}
		return nil
	})
}

func waitForActivityGone(ctx context.Context, activityID, runID string) error {
	return waitUntil(func() error {
		_, err := ws.DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  namespace,
			ActivityId: activityID,
			RunId:      runID,
		})
		var nf *serviceerror.NotFound
		if errors.As(err, &nf) {
			return nil
		}
		if err != nil {
			return err
		}
		return fmt.Errorf("activity %s still exists after delete", activityID)
	})
}

func waitForWorkflowCount(ctx context.Context, query string, want int) error {
	return waitUntil(func() error {
		resp, err := ws.CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: namespace, Query: query})
		if err != nil {
			return err
		}
		if resp.GetCount() != int64(want) {
			return fmt.Errorf("count(%q) = %d, want %d", query, resp.GetCount(), want)
		}
		return nil
	})
}

func waitForActivityCount(ctx context.Context, query string, want int) error {
	return waitUntil(func() error {
		resp, err := ws.CountActivityExecutions(ctx, &workflowservice.CountActivityExecutionsRequest{Namespace: namespace, Query: query})
		if err != nil {
			return err
		}
		if resp.GetCount() != int64(want) {
			return fmt.Errorf("count(%q) = %d, want %d", query, resp.GetCount(), want)
		}
		return nil
	})
}

// waitUntil polls fn until it returns nil or the timeout elapses, returning
// fn's last error on timeout.
func waitUntil(fn func() error) error {
	deadline := time.Now().Add(batchWaitTimeout)
	var last error
	for {
		if last = fn(); last == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s: %w", batchWaitTimeout, last)
		}
		time.Sleep(pollInterval)
	}
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
