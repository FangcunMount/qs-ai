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
		RunID, Action, Decision string
		OrgID, Version          int64
		Allowed, Confirm        bool
	}
	if json.NewDecoder(os.Stdin).Decode(&input) != nil {
		os.Exit(2)
	}
	client, connection, err := infra.DialEvaluationManagement(os.Args[1], os.Args[2], os.Args[3], os.Args[4])
	if err != nil {
		os.Exit(3)
	}
	defer func() { _ = connection.Close() }()
	snapshot := &authz.Snapshot{}
	if input.Allowed {
		snapshot.EffectiveRoles = []string{"qs:admin"}
		snapshot.Permissions = []authz.Permission{{Resource: "qs:*:*:*", Action: "*", Mode: authz.AuthorizationModeUnconditional}}
	}
	ctx := authz.WithSnapshot(context.Background(), snapshot)
	service := &app.EvaluationAdministration{Gateway: client}
	scope := app.EvaluationScope{RunID: input.RunID, OrganizationID: input.OrgID, OperatorUserID: 42}
	var result app.EvaluationState
	if input.Action == "resolve" {
		result, err = service.Resolve(ctx, scope, app.UnknownResolution{ExpectedVersion: input.Version, ExecutionID: "execution:dead", Decision: input.Decision, Reason: "跨进程管理测试", Confirm: input.Confirm, AcknowledgedDuplicateCallAndCostRisk: input.Confirm})
	} else {
		result, err = service.Get(ctx, scope)
	}
	outcome := struct {
		State           app.EvaluationState
		Code            string
		Denied, Invalid bool
	}{State: result, Code: status.Code(err).String(), Denied: errors.Is(err, app.ErrGovernanceDenied), Invalid: errors.Is(err, app.ErrInvalid)}
	if json.NewEncoder(os.Stdout).Encode(outcome) != nil {
		os.Exit(4)
	}
}
