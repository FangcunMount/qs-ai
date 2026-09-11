// Test-only adapter from migration JSON to the original Go domain and assembler.
package main

import (
	"encoding/json"
	"os"
	"strconv"
	"time"

	input "github.com/FangcunMount/qs-server/internal/apiserver/application/interpretation/aiexplanation/input"
	"github.com/FangcunMount/qs-server/internal/apiserver/application/interpretation/aiexplanation/source"
	profile "github.com/FangcunMount/qs-server/internal/apiserver/domain/interpretation/aiexplanation/profile"
	"github.com/FangcunMount/qs-server/internal/apiserver/domain/interpretation/policy"
	report "github.com/FangcunMount/qs-server/internal/apiserver/domain/interpretation/report"
	"github.com/FangcunMount/qs-server/internal/apiserver/domain/modelcatalog"
	"github.com/FangcunMount/qs-server/internal/apiserver/port/evaluationfact"
	"github.com/FangcunMount/qs-server/internal/pkg/meta"
)

type dimension struct {
	Code        string             `json:"code"`
	Kind        string             `json:"kind"`
	Name        string             `json:"name"`
	RawScore    float64            `json:"raw_score"`
	MaxScore    *float64           `json:"max_score"`
	Derived     []input.ScoreValue `json:"derived_scores"`
	Level       *input.ResultLevel `json:"level"`
	Norm        *input.NormContext `json:"norm_reference"`
	Description string             `json:"description"`
	Suggestion  string             `json:"suggestion"`
	Role        string             `json:"role"`
	Parent      string             `json:"parent_code"`
	Hierarchy   int                `json:"hierarchy_level"`
	Order       int                `json:"sort_order"`
}

func main() {
	var request struct {
		Definition profile.Definition
		Focus      []string
		Snapshot   struct {
			Source      input.Source       `json:"source"`
			Model       input.Model        `json:"model"`
			Runtime     input.Runtime      `json:"runtime"`
			Primary     *input.ScoreValue  `json:"primary_score"`
			Level       *input.ResultLevel `json:"level"`
			Conclusion  string             `json:"conclusion"`
			Dimensions  []dimension        `json:"dimensions"`
			Suggestions []struct {
				Index    int     `json:"source_index"`
				Category string  `json:"category"`
				Content  string  `json:"content"`
				Code     *string `json:"dimension_code"`
			} `json:"suggestions"`
			Extra *report.ModelExtra `json:"model_extra"`
		}
	}
	if err := json.NewDecoder(os.Stdin).Decode(&request); err != nil {
		panic(err)
	}
	s := request.Snapshot
	content := report.Content{Model: report.ModelIdentity(s.Model), PrimaryScore: (*report.ScoreValue)(s.Primary), Level: (*report.ResultLevel)(s.Level), Conclusion: s.Conclusion, ModelExtra: s.Extra}
	codes := []string{}
	for _, d := range s.Dimensions {
		derived := []report.ScoreValue{}
		for _, value := range d.Derived {
			derived = append(derived, report.ScoreValue(value))
		}
		content.Dimensions = append(content.Dimensions, report.NewNeutralDimensionInterpret(report.NewDimensionCode(d.Code), report.DimensionKind(d.Kind), d.Name, d.RawScore, d.MaxScore, (*report.ResultLevel)(d.Level), d.Description, d.Suggestion).WithHierarchy(d.Role, d.Parent, d.Hierarchy, d.Order).WithScoreContext(derived, (*report.ResultLevel)(d.Level), (*report.NormReference)(d.Norm)))
		codes = append(codes, d.Code)
	}
	visibility := report.NewFrozenPresentationProfile(codes)
	content.PresentationProfile = &visibility
	for _, suggestion := range s.Suggestions {
		for len(content.Suggestions) <= suggestion.Index {
			code := report.FactorCode("hidden")
			content.Suggestions = append(content.Suggestions, report.Suggestion{Category: "dimension", Content: "hidden", FactorCode: &code})
		}
		content.Suggestions[suggestion.Index] = report.Suggestion{Category: report.SuggestionCategory(suggestion.Category), Content: suggestion.Content, FactorCode: (*report.FactorCode)(suggestion.Code)}
	}
	id := func(value string) meta.ID {
		number, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			panic(err)
		}
		return meta.FromUint64(number)
	}
	at, err := time.Parse(time.RFC3339Nano, s.Source.GeneratedAt)
	if err != nil {
		panic(err)
	}
	r, err := report.RestoreInterpretReport(report.InterpretReportInput{ID: id(s.Source.ReportID), OutcomeID: id(s.Source.OutcomeID), GenerationID: meta.ID(100), InterpretationRunID: meta.ID(102), Association: report.Association{OrgID: 1, AssessmentID: meta.ID(42), TesteeID: 7}, ReportType: policy.ReportType(s.Source.ReportType), TemplateVersion: policy.TemplateVersion(s.Source.TemplateVersion), ContentSchemaVersion: s.Source.ContentSchemaVersion, BuilderIdentity: s.Source.BuilderIdentity, GeneratedAt: at, Content: content})
	if err != nil {
		panic(err)
	}
	outcome := evaluationfact.NewRecord(evaluationfact.NewRecordInput{ID: r.OutcomeID(), OrgID: 1, AssessmentID: meta.ID(42), TesteeID: 7, Model: evaluationfact.ModelIdentity{Kind: modelcatalog.Kind(s.Model.Kind), Algorithm: modelcatalog.Algorithm(s.Model.Algorithm), Code: s.Model.Code, Version: s.Model.Version, Title: s.Model.Title}, Runtime: evaluationfact.RuntimeIdentity{DecisionKind: modelcatalog.DecisionKind(s.Runtime.DecisionKind)}})
	p, err := profile.NewDraft(meta.ID(501), request.Definition, at)
	if err != nil {
		panic(err)
	}
	if err := p.Publish(meta.ID(502), "fixture", "offline parity", at); err != nil {
		panic(err)
	}
	result, err := input.Assemble(input.Request{Source: &source.Current{Report: r, Outcome: outcome}, Profile: p, Audience: policy.AudienceParticipant, Locale: "zh-CN", FocusAreas: request.Focus})
	if err != nil {
		panic(err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result.Document); err != nil {
		panic(err)
	}
}
