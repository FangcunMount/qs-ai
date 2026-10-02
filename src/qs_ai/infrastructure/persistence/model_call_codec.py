import hashlib
import json
from dataclasses import asdict, fields, replace

from pydantic import TypeAdapter

from qs_ai.application.execution.generation import FrozenGeneration
from qs_ai.application.interpretation.input import (
    InputPolicy,
    MBTIInputPolicy,
    MBTIThematicInputPolicy,
)
from qs_ai.application.interpretation.model_route_v2 import ModelRouteV2
from qs_ai.application.interpretation.provider import ModelResponse, ModelRoute
from qs_ai.infrastructure.qs_server.profiles import decode_published_profile

_REQUEST = TypeAdapter(FrozenGeneration)
_RESPONSE = TypeAdapter(ModelResponse)


class JSONModelCallCodec:
    def encode_request(self, request: FrozenGeneration) -> str:
        raw = _REQUEST.dump_json(request).decode()
        policy = request.prepared.release.input_policy
        if isinstance(policy, MBTIInputPolicy):
            document = json.loads(raw)
            document["prepared"]["release"]["input_policy"] = asdict(policy)
            return json.dumps(document, ensure_ascii=False, separators=(",", ":"))
        return raw

    def decode_request(self, raw: str) -> FrozenGeneration:
        # Validate the explicit version separately: a failed union branch must
        # never silently discard v2 fields and reinterpret a corrupt route as v1.
        document = json.loads(raw)
        value = document["route"]
        kind = ModelRouteV2 if "format_version" in value else ModelRoute
        if not isinstance(value, dict) or set(value) - {f.name for f in fields(kind)}:
            raise ValueError("Unexpected frozen route fields")
        route = TypeAdapter(kind).validate_json(json.dumps(value), strict=True)
        frozen = replace(_REQUEST.validate_json(raw, strict=True), route=route)
        release = document["prepared"]["release"]
        policy = release["input_policy"]
        definition = json.loads(release["definition_json"])
        policy_type: type[InputPolicy] = InputPolicy
        if definition.get("schema_version") == "ai-explanation-profile/v3":
            policy_type = MBTIThematicInputPolicy
        elif definition.get("schema_version") == "ai-explanation-profile/v2":
            policy_type = MBTIInputPolicy
        if not isinstance(policy, dict) or set(policy) != {f.name for f in fields(policy_type)}:
            raise ValueError("Unexpected frozen input policy fields")
        if policy_type is not InputPolicy:
            original = decode_published_profile(
                {
                    "definition": definition,
                    "fingerprint": policy["profile_fingerprint"],
                    "status": "published",
                }
            )
            decoded = TypeAdapter(policy_type).validate_json(json.dumps(policy), strict=True)
            if original.input_policy != decoded:
                raise ValueError("Frozen MBTI input policy differs from Profile")
            frozen = replace(
                frozen,
                prepared=replace(
                    frozen.prepared, release=replace(frozen.prepared.release, input_policy=decoded)
                ),
            )
            if isinstance(decoded, MBTIThematicInputPolicy):
                from qs_ai.application.interpretation.mbti_themes_input import (
                    validate_mbti_themes_projection,
                )

                prepared = frozen.prepared
                assembled = prepared.assembled_input
                payload = json.loads(assembled.provider_payload)
                validate_mbti_themes_projection(payload, decoded)
                canonical = json.loads(assembled.canonical_json)
                if (
                    canonical.get("schema_version") != "ai-explanation-input/v3"
                    or canonical.get("scene_contract_version") != decoded.scene_contract_version
                    or any(canonical.get(key) != payload[key] for key in payload)
                    or assembled.fingerprint
                    != "sha256:" + hashlib.sha256(assembled.canonical_json.encode()).hexdigest()
                    or json.loads(prepared.messages.data_json) != payload
                    or prepared.release != original
                ):
                    raise ValueError("Frozen MBTI thematic input differs from accepted references")
        return frozen

    def encode_response(self, response: ModelResponse) -> str:
        return _RESPONSE.dump_json(
            response,
            exclude={"execution_identity"} if response.execution_identity is None else None,
        ).decode()

    def decode_response(self, raw: str) -> ModelResponse:
        return _RESPONSE.validate_json(raw, strict=True)
