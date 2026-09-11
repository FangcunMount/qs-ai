// Test-only bridge to the pinned original renderer. No database or model calls.
package main

import (
	"context"
	"encoding/json"
	"os"

	input "github.com/FangcunMount/qs-server/internal/apiserver/application/interpretation/aiexplanation/input"
	prompt "github.com/FangcunMount/qs-server/internal/apiserver/application/interpretation/aiexplanation/prompt"
	profile "github.com/FangcunMount/qs-server/internal/apiserver/domain/interpretation/aiexplanation/profile"
	catalog "github.com/FangcunMount/qs-server/internal/apiserver/infra/aiexplanation/prompt"
)

func main() {
	var request struct {
		Definition profile.Definition
		Payload    json.RawMessage
	}
	if err := json.NewDecoder(os.Stdin).Decode(&request); err != nil {
		panic(err)
	}
	var document input.Document
	if err := json.Unmarshal(request.Payload, &document); err != nil {
		panic(err)
	}
	result := map[string]prompt.Messages{}
	for _, version := range []string{"v1", "v2", "v3", "v4", "v5", "v6"} {
		pkg, err := catalog.NewCatalog().ResolvePromptPackage(context.Background(), catalog.ParticipantScaleTemplateID, version)
		if err != nil {
			panic(err)
		}
		request.Definition.GenerationPolicy.PromptVersion = version
		messages, err := prompt.Render(pkg, request.Definition, &input.Result{Document: document, ProviderPayload: request.Payload})
		if err != nil {
			panic(err)
		}
		result[version] = messages
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		panic(err)
	}
}
