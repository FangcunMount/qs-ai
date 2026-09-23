// Actual QS client and mTLS; report facts are supplied by isolated tests.
package main

import (
	"context"
	"encoding/json"
	"errors"
	app "github.com/FangcunMount/qs-server/internal/apiserver/application/aibridge"
	infra "github.com/FangcunMount/qs-server/internal/apiserver/infra/aibridge"
	"os"
)

func main() {
	var input struct {
		Actor         app.Actor
		TesteeID      string
		AssessmentIDs []string
		Evidence      []app.EvidenceItem
	}
	if json.NewDecoder(os.Stdin).Decode(&input) != nil {
		os.Exit(2)
	}
	clients, err := infra.DialGovernance(os.Args[1], os.Args[2], os.Args[3], os.Args[4])
	if err != nil {
		os.Exit(3)
	}
	defer clients.Connection.Close()
	result, err := clients.Commands.CheckEligibility(context.Background(), input.Actor, input.TesteeID, input.AssessmentIDs, input.Evidence)
	code := "OK"
	if err != nil {
		code = "UNAVAILABLE"
		if errors.Is(err, app.ErrAccessDenied) {
			code = "PERMISSION_DENIED"
		}
	}
	_ = json.NewEncoder(os.Stdout).Encode(struct {
		Code   string
		Result *app.Eligibility
	}{code, result})
}
