// Actual QS application and TLS adapter; the request IAM snapshot is synthetic.
package main

import (
	"context"
	"encoding/json"
	"errors"
	app "github.com/FangcunMount/qs-server/internal/apiserver/application/aibridge"
	authz "github.com/FangcunMount/qs-server/internal/apiserver/application/authz"
	infra "github.com/FangcunMount/qs-server/internal/apiserver/infra/aibridge"
	"google.golang.org/grpc/status"
	"os"
)

func main() {
	var input struct {
		Action             string
		OrgID, UserID      int64
		Allowed, AuditOnly bool
		Publish            app.PublishConfiguration
		Rollback           app.RollbackPublication
		Disable            app.PublicationCommand
		Selector           app.PublicationSelector
		CommandID          string
	}
	if json.NewDecoder(os.Stdin).Decode(&input) != nil {
		os.Exit(2)
	}
	_, client, connection, err := infra.DialGovernanceManagement(os.Args[1], os.Args[2], os.Args[3], os.Args[4])
	if err != nil {
		os.Exit(3)
	}
	defer connection.Close()
	snapshot := &authz.Snapshot{}
	if input.Allowed {
		snapshot.Permissions = []authz.Permission{{Resource: "qs:*:*:*", Action: "*", Mode: authz.AuthorizationModeUnconditional}}
	}
	if input.Allowed && input.AuditOnly {
		snapshot.Permissions = []authz.Permission{{Resource: "qs:evaluation:collection:reports", Action: "audit", Mode: authz.AuthorizationModeUnconditional}}
	}
	ctx := authz.WithSnapshot(context.Background(), snapshot)
	scope := app.PublicationScope{OrganizationID: input.OrgID, OperatorUserID: input.UserID}
	service := &app.PublicationAdministration{Gateway: client}
	var result any
	switch input.Action {
	case "publish":
		result, err = service.Publish(ctx, scope, input.Publish)
	case "rollback":
		result, err = service.Rollback(ctx, scope, input.Rollback)
	case "disable":
		result, err = service.Disable(ctx, scope, input.Disable)
	case "get":
		result, err = service.Get(ctx, scope, input.Selector)
	case "receipt":
		result, err = service.GetReceipt(ctx, scope, input.CommandID)
	default:
		os.Exit(2)
	}
	if json.NewEncoder(os.Stdout).Encode(struct {
		State                     any
		Code                      string
		Denied, Invalid, Conflict bool
	}{result, status.Code(err).String(), errors.Is(err, app.ErrGovernanceDenied), errors.Is(err, app.ErrInvalid), errors.Is(err, app.ErrConflict)}) != nil {
		os.Exit(4)
	}
}
