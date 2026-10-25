"""Offline preflight uses the same final owner/type rules as runtime assembly."""

import json
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from reliable_messaging.transport import physical_topic_v2

from qs_ai.bootstrap.messaging_config import read_rocketmq_config
from qs_ai.config import MessagingOptions
from qs_ai.infrastructure.workflow_transport.messaging import ACKS, COMMANDS, EVENTS
from qs_ai.maintenance.messaging_preflight import check_keys
from tests.test_transaction_messaging import options


def change_route(path, topic, field, value):
    document = json.loads(path.read_text())
    route = next(r for r in document["routes"] if r["topic"] == topic)
    route[field] = value
    route["physical_topic"] = physical_topic_v2(
        topic, route["message_type"], route["owner"], route["route_revision"]
    )
    path.write_text(json.dumps(document))


@pytest.mark.parametrize("topic", [COMMANDS, ACKS, EVENTS])
@pytest.mark.parametrize(
    "field,value", [("owner", "qs.mongo"), ("message_type", "NORMAL"), ("route_revision", "m4")]
)
def test_final_reader_rejects_canonical_but_wrong_business_routes(tmp_path, topic, field, value):
    config = MessagingOptions.model_validate(options(tmp_path))
    change_route(tmp_path / "routes.json", topic, field, value)
    with pytest.raises(ValueError, match="Transaction workflow route binding differs"):
        read_rocketmq_config(config)


def test_offline_file_preflight_rejects_wrong_owner_before_keys_or_clients(tmp_path, monkeypatch):
    value = options(tmp_path)
    value["enabled"] = True
    value["signing_key_file"] = "/run/qs-ai-jose/ai.sign.v1.json"
    value["decrypt_key_files"] = {"ai.encrypt.v1": "/run/qs-ai-jose/ai.encrypt.v1.json"}
    value["qs_signer_files"] = {"qs.sign.v1": "/run/qs-ai-jose/qs.sign.v1.json"}
    value["qs_recipient_key_file"] = "/run/qs-ai-jose/qs.encrypt.v1.json"
    config = MessagingOptions.model_validate(value)
    change_route(tmp_path / "routes.json", COMMANDS, "owner", "qs.mongo")
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "disposable-preflight-ca")])
    now = datetime.now(UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    (tmp_path / "ca.crt").write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    for path in tmp_path.iterdir():
        path.chmod(0o640)
    monkeypatch.setenv("GRPC_DEFAULT_SSL_ROOTS_FILE_PATH", str(tmp_path / "ca.crt"))
    read_key = Mock(side_effect=AssertionError("key loading must follow route validation"))
    monkeypatch.setattr("qs_ai.maintenance.messaging_preflight.read_key", read_key)
    with pytest.raises(ValueError, match="Transaction workflow route binding differs"):
        check_keys(config)
    read_key.assert_not_called()
