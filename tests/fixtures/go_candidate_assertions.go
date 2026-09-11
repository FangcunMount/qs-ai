// Test-only parity bridge; no provider, database or production access.
package main

import (
    "context"
    "encoding/json"
    "os"
    evaluation "github.com/FangcunMount/qs-server/internal/apiserver/application/interpretation/aiexplanation/evaluation"
    input "github.com/FangcunMount/qs-server/internal/apiserver/application/interpretation/aiexplanation/input"
    profile "github.com/FangcunMount/qs-server/internal/apiserver/domain/interpretation/aiexplanation/profile"
    safety "github.com/FangcunMount/qs-server/internal/apiserver/infra/aiexplanation/safety"
)

func main() {
    var requests []struct {
        Input input.Document
        Definition profile.Definition
        Assertions []evaluation.Assertion
        Output json.RawMessage
    }
    if err := json.NewDecoder(os.Stdin).Decode(&requests); err != nil { panic(err) }
    results := [][]string{}
    for _, request := range requests {
        report, err := evaluation.EvaluateCandidate(context.Background(), request.Output, request.Input, request.Definition, request.Assertions, safety.NewDeterministicGate())
        if err != nil { panic(err) }
        statuses := []string{}
        for _, assertion := range report.Assertions { statuses = append(statuses, string(assertion.Status)) }
        results = append(results, statuses)
    }
    if err := json.NewEncoder(os.Stdout).Encode(results); err != nil { panic(err) }
}
