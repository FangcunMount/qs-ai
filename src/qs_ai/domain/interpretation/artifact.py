from dataclasses import dataclass


@dataclass(frozen=True)
class ArtifactCandidate:
    id: str
    session_id: str
    run_id: str
    evidence_set_id: str
    evidence_fingerprint: str
    invocation_id: str
    provider_request_id: str
    content_json: str
    content_fingerprint: str
    input_fingerprint: str
    profile_id: str
    profile_version: str
    profile_fingerprint: str
    prompt_fingerprint: str
    route_fingerprint: str
    output_validator_version: str
    safety_validator_version: str
    assessment_id: str
    report_id: str
    source_version: str
    schema_version: str = "qs-ai-artifact/v1"
