"""Historical test helpers retained for mTLS probes; no legacy delivery harness."""

from datetime import UTC, datetime, timedelta

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


def certificates(path):
    root_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test-ca")])

    def issue(name, key, ca=False):
        return (
            x509.CertificateBuilder()
            .subject_name(name)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.now(UTC) - timedelta(minutes=1))
            .not_valid_after(datetime.now(UTC) + timedelta(hours=1))
            .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
            .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False)
            .sign(root_key, hashes.SHA256())
        )

    (path / "ca.pem").write_bytes(
        issue(issuer, root_key, True).public_bytes(serialization.Encoding.PEM)
    )
    for short, common in [
        ("qs", "qs-apiserver.svc"),
        ("ai", "qs-ai.svc"),
        ("other", "untrusted.svc"),
    ]:
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common)])
        (path / f"{short}.pem").write_bytes(
            issue(name, key).public_bytes(serialization.Encoding.PEM)
        )
        (path / f"{short}.key").write_bytes(
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )


async def stop(process):
    if process.returncode is None:
        process.kill()
    await process.communicate()
