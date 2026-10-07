package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"

	pb "github.com/FangcunMount/qs-server/api/grpc/gen/aiworkflow"
	app "github.com/FangcunMount/qs-server/internal/apiserver/application/aibridge"
	authz "github.com/FangcunMount/qs-server/internal/apiserver/application/authz"
	infra "github.com/FangcunMount/qs-server/internal/apiserver/infra/aibridge"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// retiredExecutionProbe is test-only: preserve actual QS authorization/validation,
// then verify that the old RPC endpoints reject execution over the real mTLS wire.
// It does not implement Outbox submission or the current production MQ write path.
type retiredExecutionProbe struct{ connection grpc.ClientConnInterface }

func (p retiredExecutionProbe) SubmitParticipantRetry(ctx context.Context, _ app.DraftScope, _ string, _ app.ParticipantRetry) error {
	_, err := pb.NewParticipantManagementClient(p.connection).Retry(ctx, &pb.ParticipantRetryCommand{})
	return err
}
func (p retiredExecutionProbe) SubmitEvaluationStart(ctx context.Context, _ app.EvaluationScope, _ string, _ app.EvaluationStart) error {
	_, err := pb.NewEvaluationManagementClient(p.connection).Start(ctx, &pb.EvaluationStartCommand{})
	return err
}
func (p retiredExecutionProbe) SubmitEvaluationCancel(ctx context.Context, _ app.EvaluationScope, _ string, _ app.EvaluationCancel) error {
	_, err := pb.NewEvaluationManagementClient(p.connection).Cancel(ctx, &pb.EvaluationCancelCommand{})
	return err
}

func main() {
	// Reserve stdout for the JSON protocol; QS diagnostics use stderr.
	resultOutput := os.Stdout
	os.Stdout = os.Stderr
	var input struct {
		SemanticBody                                    json.RawMessage
		SemanticID, SemanticOperation, SemanticRevision string
		SemanticWrite                                   bool
		QuotaBody                                       json.RawMessage
		QuotaOperation, QuotaCommandID                  string
		QuotaWrite                                      bool
		SolutionBody                                    json.RawMessage
		SolutionID, SolutionOperation                   string
		SolutionWrite                                   bool
		ProfileLifecycle                                app.ProfileLifecycleQuery
		ProfileVersion                                  string
		SessionID, CommandID                            string
		ParticipantRetry                                app.ParticipantRetry
		RunID, Action, Decision, Reason                 string
		Release                                         app.EvaluationRelease
		Plan                                            app.EvaluationPlanQuery
		Catalog                                         app.EvaluationCatalogQuery
		Executions                                      app.ExecutionQuery
		ParticipantCapacity                             app.ParticipantCapacityQuery
		Review                                          app.EvaluationReview
		UserID                                          int64
		OrgID, Version                                  int64
		Allowed, Confirm                                bool
		AuditOnly                                       bool
		CandidateID                                     string
		ExecutionID                                     string
		ExpectedPassed                                  *bool
		Discard                                         *bool
	}
	if json.NewDecoder(os.Stdin).Decode(&input) != nil {
		os.Exit(2)
	}
	clients, err := infra.DialGovernance(os.Args[1], os.Args[2], os.Args[3], os.Args[4])
	if err != nil {
		os.Exit(3)
	}
	defer func() { _ = clients.Connection.Close() }()
	client := clients.Evaluation
	snapshot := &authz.Snapshot{}
	if input.Allowed {
		snapshot.EffectiveRoles = []string{"qs:admin"}
		snapshot.Permissions = []authz.Permission{{Resource: "qs:*:*:*", Action: "*", Mode: authz.AuthorizationModeUnconditional}}
	}
	if input.Allowed && input.AuditOnly {
		snapshot.EffectiveRoles = nil
		snapshot.Permissions = []authz.Permission{{Resource: "qs:evaluation:collection:reports", Action: "audit", Mode: authz.AuthorizationModeUnconditional}}
	}
	ctx := authz.WithSnapshot(context.Background(), snapshot)
	connection, ok := clients.Connection.(grpc.ClientConnInterface)
	if !ok {
		os.Exit(3)
	}
	retired := retiredExecutionProbe{connection}
	service := &app.EvaluationAdministration{Gateway: client, Messages: retired}
	if input.UserID == 0 {
		input.UserID = 42
	}
	scope := app.EvaluationScope{RunID: input.RunID, OrganizationID: input.OrgID, OperatorUserID: input.UserID}
	var result any
	if input.Action == "semantic-draft" {
		drafts := &app.SemanticDraftAdministration{Gateway: clients.SemanticDrafts}
		scope := app.DraftScope{OrganizationID: input.OrgID, OperatorUserID: input.UserID}
		if input.SemanticWrite {
			result, err = drafts.Write(ctx, scope, input.SemanticOperation, input.SemanticID, input.SemanticBody)
		} else {
			result, err = drafts.Read(ctx, scope, input.SemanticOperation, input.SemanticID, input.SemanticRevision)
		}
	} else if input.Action == "quota" {
		quotas := &app.QuotaAdministration{Gateway: clients.Quotas}
		scope := app.DraftScope{OrganizationID: input.OrgID, OperatorUserID: input.UserID}
		if input.QuotaWrite {
			result, err = quotas.Write(ctx, scope, input.QuotaOperation, input.QuotaBody)
		} else {
			result, err = quotas.Read(ctx, scope, input.QuotaOperation, input.QuotaCommandID, "")
		}
	} else if input.Action == "solution" {
		solutions := &app.SolutionAdministration{Gateway: clients.Solutions}
		scope := app.DraftScope{OrganizationID: input.OrgID, OperatorUserID: input.UserID}
		if input.SolutionWrite {
			result, err = solutions.Write(ctx, scope, input.SolutionOperation, input.SolutionID, input.SolutionBody)
		} else {
			result, err = solutions.Read(ctx, scope, input.SolutionOperation, input.SolutionID, "")
		}
	} else if input.Action == "profile-list" {
		result, err = (&app.ProfileAdministration{Gateway: clients.Profiles}).ListLifecycle(ctx, app.DraftScope{OrganizationID: input.OrgID, OperatorUserID: input.UserID}, input.ProfileLifecycle)
	} else if input.Action == "profile-lifecycle" {
		result, err = (&app.ProfileAdministration{Gateway: clients.Profiles}).GetLifecycle(ctx, app.DraftScope{OrganizationID: input.OrgID, OperatorUserID: input.UserID}, input.ProfileLifecycle.Identity, input.ProfileVersion)
	} else if input.Action == "participant-get" {
		result, err = (&app.ParticipantAdministration{Gateway: clients.Participants}).Get(ctx, app.DraftScope{OrganizationID: input.OrgID, OperatorUserID: input.UserID}, input.SessionID)
	} else if input.Action == "participant-retry" {
		err = (&app.ParticipantAdministration{Gateway: clients.Participants, Messages: retired}).SubmitRetry(ctx, app.DraftScope{OrganizationID: input.OrgID, OperatorUserID: input.UserID}, input.SessionID, input.ParticipantRetry)
	} else if input.Action == "participant-receipt" {
		result, err = (&app.ParticipantAdministration{Gateway: clients.Participants}).RetryReceipt(ctx, app.DraftScope{OrganizationID: input.OrgID, OperatorUserID: input.UserID}, input.CommandID)
	} else if input.Action == "participant-capacity" {
		result, err = (&app.ParticipantAdministration{Gateway: clients.Participants}).Capacity(ctx, app.DraftScope{OrganizationID: input.OrgID, OperatorUserID: input.UserID}, input.ParticipantCapacity)
	} else if input.Action == "capacity" {
		result, err = service.Capacity(ctx, app.DraftScope{OrganizationID: input.OrgID, OperatorUserID: input.UserID})
	} else if input.Action == "executions" {
		input.Executions.ExpectedVersion = input.Version
		result, err = service.ListExecutions(ctx, scope, input.Executions)
	} else if input.Action == "execution-output" {
		result, err = service.GetExecutionOutput(ctx, scope, input.Version, input.ExecutionID)
	} else if input.Action == "list" {
		result, err = service.List(ctx, app.DraftScope{OrganizationID: input.OrgID, OperatorUserID: input.UserID}, input.Catalog)
	} else if input.Action == "cancel" {
		err = service.SubmitCancel(ctx, scope, app.EvaluationCancel{CommandID: "11111111-1111-4111-8111-111111111111", ExpectedVersion: input.Version, Reason: input.Reason, Confirm: input.Confirm, Discard: input.Discard})
	} else if input.Action == "unknowns" {
		result, err = service.ListUnknowns(ctx, scope, input.Version)
	} else if input.Action == "prepare" {
		result, err = service.Prepare(ctx, app.DraftScope{OrganizationID: input.OrgID, OperatorUserID: input.UserID}, input.Plan)
	} else if input.Action == "reopen" {
		result, err = service.ReopenReview(ctx, scope, app.EvaluationReopen{ExpectedVersion: input.Version, Reason: input.Reason, Confirm: input.Confirm})
	} else if input.Action == "finalize" {
		result, err = service.Finalize(ctx, scope, app.EvaluationFinalize{ExpectedVersion: input.Version, ExpectedPassed: input.ExpectedPassed, Reason: input.Reason, Confirm: input.Confirm})
	} else if input.Action == "gates" {
		result, err = service.PreviewGates(ctx, scope, input.Version)
	} else if input.Action == "candidates" {
		result, err = service.ListCandidates(ctx, scope)
	} else if input.Action == "candidate" {
		result, err = service.GetCandidate(ctx, scope, app.CandidateQuery{CandidateID: input.CandidateID, ExpectedVersion: input.Version})
	} else if input.Action == "review" {
		input.Review.ExpectedVersion = input.Version
		result, err = service.Review(ctx, scope, input.Review)
	} else if input.Action == "create" {
		result, err = service.Create(ctx, scope, app.EvaluationCreate{Release: input.Release, Reason: input.Reason, Confirm: input.Confirm})
	} else if input.Action == "resolve" {
		if input.ExecutionID == "" {
			input.ExecutionID = "execution:dead"
		}
		result, err = service.Resolve(ctx, scope, app.UnknownResolution{ExpectedVersion: input.Version, ExecutionID: input.ExecutionID, Decision: input.Decision, Reason: "跨进程管理测试", Confirm: input.Confirm, AcknowledgedDuplicateCallAndCostRisk: input.Confirm})
	} else if input.Action == "start" {
		err = service.SubmitStart(ctx, scope, app.EvaluationStart{CommandID: "11111111-1111-4111-8111-111111111111", ExpectedVersion: input.Version, Reason: "跨进程管理测试", Confirm: input.Confirm})
	} else {
		result, err = service.Get(ctx, scope)
	}
	outcome := struct {
		State           any
		Code            string
		Denied, Invalid bool
	}{State: result, Code: status.Code(err).String(), Denied: errors.Is(err, app.ErrGovernanceDenied), Invalid: errors.Is(err, app.ErrInvalid)}
	if json.NewEncoder(resultOutput).Encode(outcome) != nil {
		os.Exit(4)
	}
}
