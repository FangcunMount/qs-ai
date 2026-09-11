// Test-only deterministic validation comparison. Does not run semantic safety.
package main

import (
	"context"
	"encoding/json"
	input "github.com/FangcunMount/qs-server/internal/apiserver/application/interpretation/aiexplanation/input"
	port "github.com/FangcunMount/qs-server/internal/apiserver/application/interpretation/aiexplanation/port"
	validation "github.com/FangcunMount/qs-server/internal/apiserver/application/interpretation/aiexplanation/validation"
	profile "github.com/FangcunMount/qs-server/internal/apiserver/domain/interpretation/aiexplanation/profile"
	safety "github.com/FangcunMount/qs-server/internal/apiserver/infra/aiexplanation/safety"
	"os"
)

func main() {
	var request struct {
		Input      input.Document
		Definition profile.Definition
		Candidates []json.RawMessage
		Safety     bool
	}
	if err := json.NewDecoder(os.Stdin).Decode(&request); err != nil {
		panic(err)
	}
	results := []bool{}
	for _, raw := range request.Candidates {
		validated, err := validation.Validate(raw, request.Input, request.Definition)
		allowed := err == nil
		if allowed && request.Safety {
			result, err := safety.NewDeterministicGate().Evaluate(context.Background(), port.SafetyRequest{Content: validated.Content, Policy: request.Definition.SafetyPolicy})
			allowed = err == nil && result.Allowed
		}
		results = append(results, allowed)
	}
	if err := json.NewEncoder(os.Stdout).Encode(results); err != nil {
		panic(err)
	}
}
