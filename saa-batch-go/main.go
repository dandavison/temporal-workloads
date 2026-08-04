// Command saa-batch-go manually exercises the Standalone Activity (SAA) batch
// operations added in temporal PR #10803 / api PR #806 / cli PR #1100:
// batch Terminate, Cancel, and Delete of standalone activities, targeted
// either by visibility query or by an explicit list of executions
// (ArchetypeExecutions). This support is unreleased, so it requires a server
// built from that branch and an api-go checkout that has the new proto
// fields; see the go.mod replace directive.
//
// Prerequisites (one-time):
//
//	cd ~/worktrees/cli/ks--saa-batch-cmds/cli
//	go build -o /tmp/temporal-saa-batch ./cmd/temporal
//	/tmp/temporal-saa-batch server start-dev --headless
//
// Usage:
//
//	go run . [-namespace default]
//
// Env vars: TEMPORAL_ADDRESS (default localhost:7233), TEMPORAL_NAMESPACE
// (default "default").
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	activitiesPerCase = 3
	activityTaskQueue = "saa-batch-workload-tq"
	activityTimeout   = 10 * time.Minute
	batchWaitTimeout  = 30 * time.Second
	batchPollInterval = 300 * time.Millisecond
)

type opKind int

const (
	opTerminate opKind = iota
	opCancel
	opDelete
)

func (k opKind) String() string {
	switch k {
	case opTerminate:
		return "terminate"
	case opCancel:
		return "cancel"
	case opDelete:
		return "delete"
	default:
		return "unknown"
	}
}

func (k opKind) batchOperationType() enumspb.BatchOperationType {
	switch k {
	case opTerminate:
		return enumspb.BATCH_OPERATION_TYPE_TERMINATE_ACTIVITY
	case opCancel:
		return enumspb.BATCH_OPERATION_TYPE_CANCEL_ACTIVITY
	case opDelete:
		return enumspb.BATCH_OPERATION_TYPE_DELETE_ACTIVITY
	default:
		return enumspb.BATCH_OPERATION_TYPE_UNSPECIFIED
	}
}

// setOperation sets the StartBatchOperationRequest oneof for this kind.
// (The oneof's interface type is unexported, so it can only be satisfied by
// assigning a concrete literal directly to the field, not by returning one
// from a helper.)
func (k opKind) setOperation(req *workflowservice.StartBatchOperationRequest) {
	const identity = "saa-batch-workload"
	switch k {
	case opTerminate:
		req.Operation = &workflowservice.StartBatchOperationRequest_TerminateActivitiesOperation{
			TerminateActivitiesOperation: &batchpb.BatchOperationTerminateActivities{
				Identity: identity,
				Reason:   "saa-batch-workload manual exercise",
			},
		}
	case opCancel:
		req.Operation = &workflowservice.StartBatchOperationRequest_CancelActivitiesOperation{
			CancelActivitiesOperation: &batchpb.BatchOperationCancelActivities{
				Identity: identity,
				Reason:   "saa-batch-workload manual exercise",
			},
		}
	case opDelete:
		req.Operation = &workflowservice.StartBatchOperationRequest_DeleteActivitiesOperation{
			DeleteActivitiesOperation: &batchpb.BatchOperationDeleteActivities{},
		}
	default:
		panic(fmt.Sprintf("unhandled op kind %v", k))
	}
}

type targetMode int

const (
	modeQuery targetMode = iota
	modeArchetypeExecutions
)

func (m targetMode) String() string {
	switch m {
	case modeQuery:
		return "query"
	case modeArchetypeExecutions:
		return "archetype-executions"
	default:
		return "unknown"
	}
}

type activityTarget struct {
	id    string
	runID string
}

func main() {
	namespace := flag.String("namespace", envOr("TEMPORAL_NAMESPACE", "default"), "namespace to run against")
	flag.Parse()

	address := envOr("TEMPORAL_ADDRESS", "localhost:7233")
	c, err := sdkclient.Dial(sdkclient.Options{HostPort: address, Namespace: *namespace})
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial %s: %v\n", address, err)
		os.Exit(1)
	}
	defer c.Close()
	ws := c.WorkflowService()
	ctx := context.Background()

	kinds := []opKind{opTerminate, opCancel, opDelete}
	modes := []targetMode{modeQuery, modeArchetypeExecutions}

	failed := false
	for _, kind := range kinds {
		for _, mode := range modes {
			label := fmt.Sprintf("%-10s via %-20s", kind, mode)
			if err := runCase(ctx, ws, *namespace, kind, mode); err != nil {
				fmt.Printf("FAIL  %s: %v\n", label, err)
				failed = true
			} else {
				fmt.Printf("PASS  %s\n", label)
			}
		}
	}

	if failed {
		os.Exit(1)
	}
}

// runCase starts activitiesPerCase standalone activities of a case-unique
// type, batches them through the given operation/targeting combination, and
// verifies the job's reported type/counts/targets plus each activity's
// resulting state.
func runCase(ctx context.Context, ws workflowservice.WorkflowServiceClient, namespace string, kind opKind, mode targetMode) error {
	activityType := fmt.Sprintf("SAABatchWorkload_%s_%s_%s", kind, mode, uuid.NewString()[:8])

	targets := make([]activityTarget, activitiesPerCase)
	for i := range targets {
		id := fmt.Sprintf("%s-%d", activityType, i)
		runID, err := startActivity(ctx, ws, namespace, id, activityType)
		if err != nil {
			return fmt.Errorf("start activity %s: %w", id, err)
		}
		targets[i] = activityTarget{id: id, runID: runID}
	}

	jobID := uuid.NewString()
	query := fmt.Sprintf("ActivityType='%s'", activityType)
	req := &workflowservice.StartBatchOperationRequest{
		Namespace: namespace,
		JobId:     jobID,
		Reason:    "saa-batch-workload manual exercise",
	}
	kind.setOperation(req)
	switch mode {
	case modeQuery:
		req.VisibilityQuery = query
	case modeArchetypeExecutions:
		req.ArchetypeExecutions = toExecutions(targets)
	}

	if _, err := ws.StartBatchOperation(ctx, req); err != nil {
		return fmt.Errorf("StartBatchOperation: %w", err)
	}

	desc, err := waitForBatchDone(ctx, ws, namespace, jobID)
	if err != nil {
		return err
	}

	wantType := kind.batchOperationType()
	if desc.OperationType != wantType {
		return fmt.Errorf("DescribeBatchOperation: operation type = %v, want %v", desc.OperationType, wantType)
	}
	if desc.CompleteOperationCount != int64(activitiesPerCase) || desc.FailureOperationCount != 0 {
		return fmt.Errorf("DescribeBatchOperation: complete=%d failure=%d, want complete=%d failure=0",
			desc.CompleteOperationCount, desc.FailureOperationCount, activitiesPerCase)
	}
	switch mode {
	case modeQuery:
		if desc.Query != query {
			return fmt.Errorf("DescribeBatchOperation: query = %q, want %q", desc.Query, query)
		}
	case modeArchetypeExecutions:
		if len(desc.Executions) != activitiesPerCase {
			return fmt.Errorf("DescribeBatchOperation: got %d executions, want %d", len(desc.Executions), activitiesPerCase)
		}
	}

	if err := verifyInListBatchOperations(ctx, ws, namespace, jobID, wantType); err != nil {
		return err
	}

	for _, t := range targets {
		if err := verifyTerminalState(ctx, ws, namespace, t, kind); err != nil {
			return err
		}
	}

	return nil
}

// startActivity starts a standalone activity with no worker polling its task
// queue, so it remains in the Scheduled run state (visible as Running) until
// a batch operation (or manual action) acts on it.
func startActivity(ctx context.Context, ws workflowservice.WorkflowServiceClient, namespace, id, activityType string) (runID string, err error) {
	resp, err := ws.StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:              namespace,
		RequestId:              uuid.NewString(),
		ActivityId:             id,
		ActivityType:           &commonpb.ActivityType{Name: activityType},
		TaskQueue:              &taskqueuepb.TaskQueue{Name: activityTaskQueue},
		ScheduleToCloseTimeout: durationpb.New(activityTimeout),
		StartToCloseTimeout:    durationpb.New(activityTimeout),
	})
	if err != nil {
		return "", err
	}
	return resp.RunId, nil
}

func waitForBatchDone(ctx context.Context, ws workflowservice.WorkflowServiceClient, namespace, jobID string) (*workflowservice.DescribeBatchOperationResponse, error) {
	deadline := time.Now().Add(batchWaitTimeout)
	for {
		desc, err := ws.DescribeBatchOperation(ctx, &workflowservice.DescribeBatchOperationRequest{
			Namespace: namespace,
			JobId:     jobID,
		})
		if err != nil {
			return nil, fmt.Errorf("DescribeBatchOperation: %w", err)
		}
		if desc.State != enumspb.BATCH_OPERATION_STATE_RUNNING {
			if desc.State != enumspb.BATCH_OPERATION_STATE_COMPLETED {
				return desc, fmt.Errorf("batch job %s ended in state %v, want COMPLETED", jobID, desc.State)
			}
			return desc, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("batch job %s did not complete within %s (last state %v)", jobID, batchWaitTimeout, desc.State)
		}
		time.Sleep(batchPollInterval)
	}
}

func verifyInListBatchOperations(ctx context.Context, ws workflowservice.WorkflowServiceClient, namespace, jobID string, wantType enumspb.BatchOperationType) error {
	resp, err := ws.ListBatchOperations(ctx, &workflowservice.ListBatchOperationsRequest{
		Namespace: namespace,
		PageSize:  100,
	})
	if err != nil {
		return fmt.Errorf("ListBatchOperations: %w", err)
	}
	for _, info := range resp.OperationInfo {
		if info.JobId != jobID {
			continue
		}
		if info.OperationType != wantType {
			return fmt.Errorf("ListBatchOperations: job %s has operation type %v, want %v", jobID, info.OperationType, wantType)
		}
		return nil
	}
	return fmt.Errorf("ListBatchOperations: job %s not found", jobID)
}

// verifyTerminalState checks that a targeted activity ended up where the
// batch operation should have left it: Deleted activities must 404 on
// describe; Terminated/Canceled activities must describe with that status.
func verifyTerminalState(ctx context.Context, ws workflowservice.WorkflowServiceClient, namespace string, t activityTarget, kind opKind) error {
	resp, err := ws.DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:  namespace,
		ActivityId: t.id,
		RunId:      t.runID,
	})

	if kind == opDelete {
		var notFound *serviceerror.NotFound
		if err == nil {
			return fmt.Errorf("activity %s still describable after delete", t.id)
		}
		if !errors.As(err, &notFound) {
			return fmt.Errorf("describe %s after delete: want NotFound, got %w", t.id, err)
		}
		return nil
	}

	if err != nil {
		return fmt.Errorf("describe %s: %w", t.id, err)
	}
	wantStatus := map[opKind]enumspb.ActivityExecutionStatus{
		opTerminate: enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		opCancel:    enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED,
	}[kind]
	if got := resp.Info.GetStatus(); got != wantStatus {
		return fmt.Errorf("activity %s: status = %v, want %v", t.id, got, wantStatus)
	}
	return nil
}

func toExecutions(targets []activityTarget) []*commonpb.Execution {
	execs := make([]*commonpb.Execution, len(targets))
	for i, t := range targets {
		execs[i] = &commonpb.Execution{
			Type:       enumspb.EXECUTION_TYPE_ACTIVITY,
			BusinessId: t.id,
			RunId:      t.runID,
		}
	}
	return execs
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
