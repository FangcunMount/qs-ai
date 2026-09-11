// Test-only deterministic validation comparison. Does not run semantic safety.
package main

import (
	"encoding/json"
	input "github.com/FangcunMount/qs-server/internal/apiserver/application/interpretation/aiexplanation/input"
	validation "github.com/FangcunMount/qs-server/internal/apiserver/application/interpretation/aiexplanation/validation"
	profile "github.com/FangcunMount/qs-server/internal/apiserver/domain/interpretation/aiexplanation/profile"
	"os"
)

func main() {
	var request struct {
		Input      input.Document
		Definition profile.Definition
		Candidates []json.RawMessage
	}
	if err := json.NewDecoder(os.Stdin).Decode(&request); err != nil {
		panic(err)
	}
	results := []bool{}
	for _, raw := range request.Candidates {
		_, err := validation.Validate(raw, request.Input, request.Definition)
		results = append(results, err == nil)
	}
	if err := json.NewEncoder(os.Stdout).Encode(results); err != nil {
		panic(err)
	}
}
