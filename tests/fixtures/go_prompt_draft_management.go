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
		Create             app.CreatePromptDraft
		Revise             app.RevisePromptDraft
		Freeze             app.FreezePromptDraft
		Suite              app.RegisterSuite
		CatalogQuery       app.AssetCatalogQuery
		CatalogGet         app.AssetCatalogGet
		Profile            app.RegisterProfile
		DraftID, CommandID string
		Revision           *int64
	}
	if json.NewDecoder(os.Stdin).Decode(&input) != nil {
		os.Exit(2)
	}
	clients, err := infra.DialGovernance(os.Args[1], os.Args[2], os.Args[3], os.Args[4])
	if err != nil {
		os.Exit(3)
	}
	defer clients.Connection.Close()
	snapshot := &authz.Snapshot{}
	if input.Allowed {
		snapshot.Permissions = []authz.Permission{{Resource: "qs:*:*:*", Action: "*", Mode: authz.AuthorizationModeUnconditional}}
	}
	if input.Allowed && input.AuditOnly {
		snapshot.Permissions = []authz.Permission{{Resource: "qs:evaluation:collection:reports", Action: "audit", Mode: authz.AuthorizationModeUnconditional}}
	}
	ctx := authz.WithSnapshot(context.Background(), snapshot)
	scope := app.DraftScope{OrganizationID: input.OrgID, OperatorUserID: input.UserID}
	service := &app.PromptDraftAdministration{Gateway: clients.PromptDrafts}
	profiles := &app.ProfileAdministration{Gateway: clients.Profiles}
	suites := &app.SuiteAdministration{Gateway: clients.Suites}
	catalog := &app.AssetCatalogAdministration{Gateway: clients.Assets}
	var result any
	switch input.Action {
	case "catalog-list":
		result, err = catalog.List(ctx, scope, input.CatalogQuery)
	case "catalog-get":
		result, err = catalog.Get(ctx, scope, input.CatalogGet)
	case "suite-register":
		result, err = suites.Register(ctx, scope, input.Suite)
	case "suite-receipt":
		result, err = suites.GetReceipt(ctx, scope, input.CommandID)
	case "profile-register":
		result, err = profiles.Register(ctx, scope, input.Profile)
	case "profile-receipt":
		result, err = profiles.GetReceipt(ctx, scope, input.CommandID)
	case "create":
		result, err = service.Create(ctx, scope, input.DraftID, input.Create)
	case "revise":
		result, err = service.Revise(ctx, scope, input.DraftID, input.Revise)
	case "lifecycle":
		result, err = service.GetLifecycle(ctx, scope, input.DraftID)
	case "get":
		result, err = service.Get(ctx, scope, input.DraftID, input.Revision)
	case "freeze":
		result, err = service.Freeze(ctx, scope, input.DraftID, input.Freeze)
	case "freeze-receipt":
		result, err = service.GetFreezeReceipt(ctx, scope, input.CommandID)
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
