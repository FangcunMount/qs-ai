package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"

	app "github.com/FangcunMount/qs-server/internal/apiserver/application/aibridge"
	authz "github.com/FangcunMount/qs-server/internal/apiserver/application/authz"
	infra "github.com/FangcunMount/qs-server/internal/apiserver/infra/aibridge"
	"google.golang.org/grpc/status"
)

func main() {
	var input struct {
		RunID, Action, Decision, Reason string
		Release                         app.EvaluationRelease
		Plan                            app.EvaluationPlanQuery
		Catalog                         app.EvaluationCatalogQuery
		Executions                      app.ExecutionQuery
		ParticipantCapacity             app.ParticipantCapacityQuery
		Review                          app.EvaluationReview
		UserID                          int64
		OrgID, Version                  int64
		Allowed, Confirm                bool
		AuditOnly                       bool
		CandidateID                     string
		ExecutionID                     string
		ExpectedPassed                  *bool
		Discard                         *bool
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
	service := &app.EvaluationAdministration{Gateway: client}
	if input.UserID == 0 {
		input.UserID = 42
	}
	scope := app.EvaluationScope{RunID: input.RunID, OrganizationID: input.OrgID, OperatorUserID: input.UserID}
	var result any
	if input.Action == "participant-capacity" {
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
		result, err = service.Cancel(ctx, scope, app.EvaluationCancel{ExpectedVersion: input.Version, Reason: input.Reason, Confirm: input.Confirm, Discard: input.Discard})
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
		result, err = service.Start(ctx, scope, app.EvaluationStart{ExpectedVersion: input.Version, Reason: "跨进程管理测试", Confirm: input.Confirm})
	} else {
		result, err = service.Get(ctx, scope)
	}
	outcome := struct {
		State           any
		Code            string
		Denied, Invalid bool
	}{State: result, Code: status.Code(err).String(), Denied: errors.Is(err, app.ErrGovernanceDenied), Invalid: errors.Is(err, app.ErrInvalid)}
	if json.NewEncoder(os.Stdout).Encode(outcome) != nil {
		os.Exit(4)
	}
}
