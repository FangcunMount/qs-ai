"""Non-secret final production binding shared by offline deployment tests."""


def material_reply(revision="m4-final-ai"):
    return {
        "status": "verified",
        "read_only": True,
        "activated": False,
        "role": "ai",
        "host": "serverA",
        "revision": revision,
        "material_root": "/var/lib/fangcun-m4-materials",
        "receipt_sha256": "d" * 64,
    }


def transaction_binding():
    value = {
        "binding_revision": "m4-final-ai",
        "enabled": True,
        "provider": "rocketmq",
        "publish_enabled": False,
        "rocketmq": {
            "endpoint": "100.118.164.39:8081",
            "credentials_file": "/run/qs-ai-rocketmq/credentials.json",
            "ca_file": "/run/qs-ai-rocketmq/ca.crt",
            "routes_file": "/run/qs-ai-rocketmq/routes.json",
        },
        "signing_key_file": "/run/qs-ai-jose/ai.sign.v1.json",
        "decrypt_key_files": {"ai.encrypt.v1": "/run/qs-ai-jose/ai.encrypt.v1.json"},
        "qs_signer_files": {"qs.sign.v1": "/run/qs-ai-jose/qs.sign.v1.json"},
        "qs_recipient_key_file": "/run/qs-ai-jose/qs.encrypt.v1.json",
        "max_in_flight": 1,
    }
    value["binding_revision"] = "m4-final-ai"
    value["transactions"] = {
        "ai.mysql": {
            "store_id": "Original-AI-Primary",
            "writer_epoch": "Original-Writer-Epoch",
            "route_revision": "m3-1",
            "limits": {
                "sends": 4,
                "branches": 16,
                "check_workers": 1,
                "check_queue": 16,
                "reconcile_batch": 100,
                "check_timeout": 2,
                "rpc_timeout": 5,
                "lease": 60,
                "reconcile_interval": 5,
            },
        }
    }
    return value
